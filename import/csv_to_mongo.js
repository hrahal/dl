"use strict";

var Conv = require("csvtojson").Converter;
var conv= new Conv({construct:false, ignoreEmpty: true});
var async = require("async");
var crypto = require("crypto");
var util = require("util");
var fs = require("fs");
var env = require('express')().get("env");
var mongo = require('mongoskin');
var config = require("../config");
var EventEmitter = require('events').EventEmitter;
var docs = new EventEmitter();
var db = mongo.db("mongodb://" + config[env].mongo.host + ":" +
    config[env].mongo.port + "/airport", {native_parser: true, keepAlive: true});
var data_file = process.argv[2];
var finished = false;
var total = 0,
    processed = 0,
    rejected = 0;
var ops = {};
var reviewsCol = db.bind("reviews");
var trackopsCol =  db.bind("track_ops");


var lib = {
    "updateTrackOps": function (stats, status, cb) {

        stats.status = status;

        trackopsCol.update({
            "fileId": stats.fileId
        }, stats, {
            "upsert": true
        }, cb);

    }
};

var processData = function (results, callback) {

    var rs = require("fs").createReadStream(data_file);

    var insert = async.cargo(function(reviews, cb) {

        /*
         * push data to mongo
         * */
        reviewsCol.bulkWrite(reviews, function(err, success) {

            console.log("inserted " + success.upsertedCount + " doc at a time");
            processed = processed + success.upsertedCount;

            if (err) {
                return cb(err);
            }
            cb();
        });

    }, 80);


    var processQueue = async.queue(function (data, done) {

        var index = data.rowIndex;
        var review = data.review;

        async.forEachOf(review, function (value, key, cb) {

            if (!value) {
                /*
                 * make sure not to pass empty attributes
                 * to DB
                 * */
                delete review[key];
            }

            if (key == "date") {
                review[key] = new Date(value).getTime();
            }

            cb();
        }, function () {

            review.review_id = index + review.author + review.author_country + review.airport_name;

            review.object_hash = crypto.createHash('sha1')
                .update(JSON.stringify(review))
                .digest('hex');


            reviewsCol.findOne({
                "object_hash": review.object_hash
            }, function (err, found) {

                if (err) {
                    return callback (err);
                }

                total += 1;

                if (!found) {

                    insert.push({
                        updateOne: {
                            filter: {
                                "review_id": review.review_id
                            },
                            update: review,
                            upsert:true
                        }
                    }, function (err) {

                        if (err) {
                            return callback (err)
                        }

                        //rs.resume();
                        done();
                    });
                } else {

                    rejected += 1;
                    console.log("duplicate doc detected, omitting record", review.author);
                    //rs.resume();
                    done();

                }
            });
        });
    }, 80);


    insert.saturated = function() {
        /*
         * if the cargo is full, pause the readstream
         * to suspend populating json data
         * and prevent filling up the memory
         * */
        rs.pause();
    };

    insert.empty = function() {
        var checkStream = setInterval(function () {
            if(rs.isPaused()) {
                rs.resume();
                clearTimeout(checkStream);
            }
        })
    };

    conv.on("record_parsed",function(review, rawData, rowIndex) {

        processQueue.push({review: review, rowIndex: rowIndex});

    });

    conv.on("end_parsed", function() {

        function kill () {
            console.log("updating file status, accept");
            lib.updateTrackOps(results.checkFileInDB, "ready", function (err, success) {

                if (err) {
                    return callback(err);
                }

                console.log("total", total, "processed", processed, "rejected", rejected);

                console.log("processing jobs finished, exiting now..");

                db.close();
                process.exit();

            });
        }
        processQueue.drain = function () {

            if (total && (total == rejected + processed)) {
                kill();
            }
            insert.drain = function() {
                kill();
            };
        };
    });

    rs.pipe(conv);
};

var readFileStats = function (callback) {

    fs.stat(data_file, function (err, stats) {

        if (err) {
            return callback(err);
        }

        stats.ctime = new Date(stats.ctime).getTime();
        stats.mtime = new Date(stats.mtime).getTime();
        stats.atime = new Date(stats.atime).getTime();
        stats.birthtime = new Date(stats.birthtime).getTime();

        callback(null, stats);
    });
};

var checkFile = function (results, callback) {

    var stats = results.readFileStats,
        filePath = data_file.split("/"),
        fileName = filePath[filePath.length - 1],
        fileId = fileName + "_" +
            new Date(stats.birthtime).getTime();

    stats.fileId = fileId;

    /*
    * check if the file is new or not
    * */
    trackopsCol.findOne({

        "fileId": fileId

    }, function(err, found) {

        if (err) {
            return callback(err);
        }

        if (found) {
            /*
            * compare the last modified dates
            * to update the records or not
            * or if the last processing was not complete
            * */

            console.log(found.status);
            console.log(found.ctime, stats.ctime);

            if ((found.ctime < stats.ctime) || (found.status && found.status === "processing")) {

                /*
                * file has been modified, update db record
                * */
                lib.updateTrackOps(stats, "processing", function (err, success) {

                    if (err) {
                        return callback(err);
                    }

                    callback(null, stats);

                });

            } else {

                /*
                * file not modified, nothing to do
                * */

                console.log("file is up to date, nothing to do. exiting..");
                process.exit();
            }

        } else {

            /*
            * register new file
            * */

            lib.updateTrackOps(stats, "processing", function (err, success) {

                if (err) {
                    return callback(err);
                }

                callback(null, stats);

            });
        }
    });
};

if (data_file) {

    ops.readFileStats = readFileStats;
    ops.checkFileInDB = ["readFileStats", checkFile];
    ops.processData = ["checkFileInDB", processData];

    async.auto(ops);

} else {

    console.log("please enter a file name to proceed");
    process.exit();

}