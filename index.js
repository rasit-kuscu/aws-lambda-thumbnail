process.env['NODE_ENV'] = 'production';
process.env['PATH'] += ':' + process.env['LAMBDA_TASK_ROOT'];

var child_process = require('child_process');
var fs = require('fs');
var util = require('util');
var path = require('path');
var AWS = require('aws-sdk');
var async = require('async');
var config = require('./config');
var s3 = new AWS.S3();
var tempDir = process.env['TEMP'] || '/tmp';

function downloadStream(bucket, file, cb) {
	console.log('Starting download');

	return s3.getObject({
		Bucket: bucket,
		Key: file
	}).on('error', function(res) {
		cb('S3 download error: ' + JSON.stringify(res));
	}).createReadStream();
}

function s3upload(params, filename, cb) {
	s3.upload(params)
		.on('httpUploadProgress', function(evt) {
			console.log(filename, 'Progress:', evt.loaded, '/', evt.total);
		})
		.send(cb);
}

function uploadFile(bucket, keyPrefix, cb) {
	console.log('Uploading', 'image/gif');

	var filename = path.join(tempDir, keyPrefix + '.gif');
	var rmFiles = [filename];
	var readStream = fs.createReadStream(filename);

	var params = {
		Bucket: bucket,
		Key: 'thumbnail-' + keyPrefix + '.gif',
		ContentType: 'image/gif'
	};

	async.waterfall([
		function(cb) {
			params.Body = readStream;
			s3upload(params, filename, cb);
		},
		function(data, cb) {
			console.log(filename, 'complete. Deleting now.');
			async.each(rmFiles, fs.unlink, cb);
		}
	], cb);
}

function ffmpegPaletteProcess(dlFileName, fileName, cb) {
	console.log('Starting FFmpeg');
	console.log('Generating palette...');
	child_process.execFile(
		'ffmpeg',
		[
			'-y',
			'-ss', '30',
			'-t', '3',
			'-i', dlFileName,
			'-vf', 'fps=10,scale=320:-1:flags=lanczos,palettegen',
			fileName + '.png'
		],
		{
			cwd: tempDir
		},
		function(err, stdout, stderr) {
			console.log('Palette has been created.');
			console.log('FFmpeg done.');
			return cb(err, 'FFmpeg finished:' + JSON.stringify({ stdout: stdout, stderr: stderr}));
		}
	);
}

function ffmpegGifProcess(dlFileName, fileName, cb) {
	console.log('Starting FFmpeg');
	console.log('Generating gif...');
	child_process.execFile(
		"ffmpeg",
		[
			"-ss", "30",
			"-t", "3",
			"-i", dlFileName,
			"-i", fileName + ".png",
			"-filter_complex", 'fps=10,scale=320:-1:flags=lanczos[x];[x][1:v]paletteuse',
			fileName + ".gif"
		],
		{
			cwd: tempDir
		},
		function(err, stdout, stderr) {
			console.log('Gif has been created.');
			console.log('FFmpeg done.');
			return cb(err, 'FFmpeg finished:' + JSON.stringify({ stdout: stdout, stderr: stderr}));
		}
	);
}

function processVideo(s3Event, srcKey, keyPrefix, cb) {
	var dlFileName = 'temp-' + srcKey;
	var dlFile = path.join(tempDir, dlFileName);
	var filePalette = path.join(tempDir, keyPrefix + '.png');

	async.series([
		function(cb) {
			var dlStream = downloadStream(s3Event.bucket.name, srcKey, cb);
			dlStream.on('end', function() {
				cb(null, 'Download finished.');
			});
			dlStream.pipe(fs.createWriteStream(dlFile));
		},
		function(cb) {
			ffmpegPaletteProcess(dlFileName, keyPrefix, cb);
		},
		function(cb) {
			ffmpegGifProcess(dlFileName, keyPrefix, cb);
		},
		function(cb) {
			console.log('Deleting downloaded file.');
			fs.unlink(dlFile, cb);
		},
		function(cb) {
			console.log('Deleting generated palette file.');
			fs.unlink(filePalette, cb);
		}
	], cb);
}

exports.handler = function(event, context) {
	console.log("Reading options from event:\n", util.inspect(event, {depth: 5}));

	var s3Event = event.Records[0].s3;
	var srcKey = decodeURIComponent(s3Event.object.key);
	var keyPrefix = srcKey.replace(/\.[^/.]+$/, '');

	async.series([
		function (cb) { processVideo(s3Event, srcKey, keyPrefix, cb); },
		function (cb) {
			var dstBucket = config.destinationBucket;
			async.parallel([
				function (cb) { uploadFile(dstBucket, keyPrefix, cb); }
			], cb);
		}
	], function(err, results) {
		if (err) context.fail(err);
		else context.succeed(results);
	});
};
