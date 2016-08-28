process.env['NODE_ENV'] = 'production';
process.env['PATH'] += ':' + process.env['LAMBDA_TASK_ROOT'];

var child_process = require('child_process');
var request = require('request');
var async = require('async');
var fs = require('fs');
var util = require('util');
var path = require('path');
var AWS = require('aws-sdk');
var gm = require('gm').subClass({
    imageMagick: true
});
var s3 = new AWS.S3();
var tempDir = process.env['TEMP'] || '/tmp';

function updateVideoDuration(keyPrefix, duration, url, username, password, cb) {
    var originalName = keyPrefix.substr(keyPrefix.lastIndexOf('/') + 1) + ".mp4";
    async.series([
        function login(next) {
            request.post(url + 'auth', {
                json: {
                    username: username,
                    password: password
                }
            }, function(error, response, body) {
                if (!error && response.statusCode == 200) {
                    next(null, response.body.token);
                } else {
                    console.log("Could not connect api!");
                    next(null);
                }
            });
        },
        function update(next, token) {
            request.post(url + 'gallery/update_video_duration', {
                json: {
                    original: originalName,
                    duration: duration
                },
                headers: {
                    'Authorization': 'Bearer ' + token
                }
            }, function(error, response, body) {
                if (!error && response.statusCode == 200) {
                    console.log(originalName + " duration has been set to " + duration + " seconds");
                } else {
                    console.log(originalName + " duration could not saved ! (" + duration + ") seconds");
                }
                next(null);
            });
        }
    ], function(err, stdout, stderr) {
      return cb(err, 'Update video duration finished:' + JSON.stringify({
          stdout: stdout,
          stderr: stderr
      }));
    });
}

function getVideoDuration(dlFileName, cb) {
    console.log('Getting video duration.');

    child_process.execFile(
        'ffprobe', [
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_format',
            dlFileName
        ], {
            cwd: tempDir
        },
        function(err, stdout, stderr) {
          return cb(null, parseDuration(stdout));
        }
    );
}

function parseDuration(json) {
    var responseDuration = 0;
    try {
        var ffprobeResponse = JSON.parse(json);
        if (typeof ffprobeResponse.format != 'undefined') {
            if (typeof ffprobeResponse.format.duration != 'undefined') {
                var rawDuration = ffprobeResponse.format.duration + '';
                var durationArr = rawDuration.split('.');
                durationArr.pop();

                if (durationArr.length > 0) {
                    responseDuration = durationArr.join('.');
                } else {
                    responseDuration = ffprobeResponse.format.duration;
                }
            }
        }
    } catch (err) {
        console.log("Could not parse video duration");
    }

    return responseDuration;
}

function setVideoDuration(dlFileName, keyPrefix, runtimeConfiguration, cb) {
  async.waterfall([
      function(cb) {
          getVideoDuration(dlFileName, cb);
      },
      function(duration, cb) {
          updateVideoDuration(keyPrefix, duration, runtimeConfiguration.api.url, runtimeConfiguration.api.username, runtimeConfiguration.api.password, cb);
      }
  ], cb);
}

function generateFileName(key, extension) {
    var filename = key.substr(key.lastIndexOf('/') + 1);
    var key = key.split('/');
    key.pop();
    var delimiter = '';
    var total = key.length;
    if (total > 0) {
        delimiter = '/';
    }
    return (key.join('/') + delimiter + 'thumbnail-' + filename + '.' + extension);
}

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
        Key: generateFileName(keyPrefix, "gif"),
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

function ffmpegPaletteProcess(dlFileName, fileName, width, cb) {
    console.log('Starting FFmpeg');
    console.log('Generating palette...');
    child_process.execFile(
        'ffmpeg', [
            '-y',
            '-ss', '30',
            '-t', '3',
            '-i', dlFileName,
            '-vf', 'fps=10,scale=' + width + ':-1:flags=lanczos,palettegen',
            fileName + '.png'
        ], {
            cwd: tempDir
        },
        function(err, stdout, stderr) {
            console.log('Palette has been created.');
            console.log('FFmpeg done.');
            return cb(err, 'FFmpeg finished:' + JSON.stringify({
                stdout: stdout,
                stderr: stderr
            }));
        }
    );
}

function ffmpegGifProcess(dlFileName, fileName, width, cb) {
    console.log('Starting FFmpeg');
    console.log('Generating gif...');
    child_process.execFile(
        "ffmpeg", [
            "-ss", "30",
            "-t", "3",
            "-i", dlFileName,
            "-i", fileName + ".png",
            "-filter_complex", 'fps=10,scale=' + width + ':-1:flags=lanczos[x];[x][1:v]paletteuse',
            fileName + ".gif"
        ], {
            cwd: tempDir
        },
        function(err, stdout, stderr) {
            console.log('Gif has been created.');
            console.log('FFmpeg done.');
            return cb(err, 'FFmpeg finished:' + JSON.stringify({
                stdout: stdout,
                stderr: stderr
            }));
        }
    );
}

function processVideo(s3Event, srcKey, keyPrefix, runtimeConfiguration, cb) {
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
            ffmpegPaletteProcess(dlFileName, keyPrefix, runtimeConfiguration.width, cb);
        },
        function(cb) {
            ffmpegGifProcess(dlFileName, keyPrefix, runtimeConfiguration.width, cb);
        },
        function(cb) {
            setVideoDuration(dlFileName, keyPrefix, runtimeConfiguration, cb);
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

function createVideoThumbnail(s3Event, srcKey, keyPrefix, dstBucket, runtimeConfiguration) {
    async.series([
        function(cb) {
            processVideo(s3Event, srcKey, keyPrefix, runtimeConfiguration, cb);
        },
        function(cb) {
            async.parallel([
                function(cb) {
                    uploadFile(dstBucket, keyPrefix, cb)
                }
            ], cb);
        },
    ], function(err, results) {
        if (err) {
            console.error(
                'Unable to create thumbnail for ' + srcKey +
                ' and upload to ' + dstBucket +
                ' due to an error: ' + err
            );
        } else {
            console.log(
                'Successfully created thumbnail for ' + srcKey +
                ' and uploaded to ' + dstBucket
            );
        }

        console.log(null, "done!");
    });
}

function createImageThumbnail(srcBucket, dstBucket, srcKey, keyPrefix, documentType, runtimeConfiguration) {
    async.waterfall([
        function download(next) {
            s3.getObject({
                    Bucket: srcBucket,
                    Key: srcKey
                },
                next);
        },
        function transform(response, next) {
            gm(response.Body).size(function(err, size) {
                this.resize(runtimeConfiguration.width)
                    .toBuffer(documentType, function(err, buffer) {
                        if (err) {
                            next(err);
                        } else {
                            next(null, response.ContentType, buffer);
                        }
                    });
            });
        },
        function upload(contentType, data, next) {
            s3.putObject({
                    Bucket: dstBucket,
                    Key: generateFileName(keyPrefix, documentType),
                    Body: data,
                    ContentType: contentType
                },
                next);
        }
    ], function(err) {
        if (err) {
            console.error(
                'Unable to resize ' + srcBucket + '/' + srcKey +
                ' and upload to ' + dstBucket +
                ' due to an error: ' + err
            );
        } else {
            console.log(
                'Successfully resized ' + srcBucket + '/' + srcKey +
                ' and uploaded to ' + dstBucket
            );
        }

        console.log(null, "done!");
    });
}

exports.handler = function(event, context) {
    console.log("Reading options from event:\n", util.inspect(event, {
        depth: 5
    }));

    var srcBucket = event.Records[0].s3.bucket.name;
    var dstBucket = srcBucket + '-thumbnail';

    var s3Event = event.Records[0].s3;
    var srcKey = decodeURIComponent(s3Event.object.key);
    var keyPrefix = srcKey.replace(/\.[^/.]+$/, '');

    var typeMatch = srcKey.match(/\.([^.]*)$/);
    if (!typeMatch) {
        console.log("Could not determine the document type.");
        return;
    }

    var documentType = typeMatch[1];
    if (documentType != "jpg" && documentType != "png" && documentType != "mp4") {
        console.log('Unsupported document type: ' + documentType);
        return;
    }

    AWS.config.apiVersions = {
        lambda: '2015-03-31'
    };

    var lambda = new AWS.Lambda();
    var params = {
        FunctionName: context.functionName
    };
    lambda.getFunction(params, function(err, data) {
        if (!err) {
            try {
                var runtimeConfiguration = {};
                runtimeConfiguration = JSON.parse(data.Configuration.Description);

                if (documentType === "mp4") {
                    createVideoThumbnail(s3Event, srcKey, keyPrefix, dstBucket, runtimeConfiguration)
                } else if (documentType === "jpg" || documentType === "png") {
                    createImageThumbnail(srcBucket, dstBucket, srcKey, keyPrefix, documentType, runtimeConfiguration)
                }
            } catch (except) {
                console.log("Unable to parse description as JSON");
            }

            return runtimeConfiguration;
        } else {
            console.log("Could not read configuration");
        }
    });
};
