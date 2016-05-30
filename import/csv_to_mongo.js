"use strict";

var Conv = require("csvtojson").Converter,
    conv= new Conv({construct:false, ignoreEmpty: true}),
    async = require("async"),
    crypto = require("crypto"),
    util = require("util"),
    fs = require("fs"),
    env = require('express')().get("env"),
    mongo = require('mongoskin'),
    config = require("../config"),
    db = mongo.db("mongodb://" + config[env].mongo.host + ":" +
        config[env].mongo.port + "/airport", {native_parser: true, keepAlive: true}),
    reviewsCol = db.bind("reviews"),
    trackopsCol =  db.bind("track_ops"),
    data_file = process.argv[2],
    total = 0,
    processed = 0,
    rejected = 0,
    checkEndStream;

var lib = {
    "updateTrackOps": function (stats, status, cb) {

        stats.status = status;

        trackopsCol.update({
            "fileId": stats.fileId
        }, stats, {
            "upsert": true
        }, cb);

    },
    "kill": function (stats, status) {

        clearTimeout(checkEndStream);
        console.log("updating file status, accept");
        lib.updateTrackOps(stats, status, function (err, success) {

            if (err) {
                return callback(err);
            }

            console.log("total", total, "processed", processed, "rejected", rejected);
            console.log("processing jobs finished, exiting now..");

            db.close();
            process.exit();

        });
    },
    "createObjectHash": function (item) {
        return crypto.createHash('sha1')
            .update(JSON.stringify(item))
            .digest('hex');
    }
};

var processData = function (results, callback) {

    var rs = fs.createReadStream(data_file);

    var insert = async.cargo(function(reviews, cb) {

        /*
         * push bulk data to mongo
         * */
        reviewsCol.bulkWrite(reviews, function(err, success) {

            console.log("inserted " + reviews.length + " doc at a time");
            processed = processed + reviews.length;

            if (err) {
                return cb(err);
            }
            cb();
        });

    }, 100);


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

            var review_id = index +
                review.author + review.author_country +
                review.airport_name;


            review.review_id = review_id.replace(/[^a-z0-9]/gi, '').toLowerCase();

            review.object_hash = lib.createObjectHash(review);

            reviewsCol.findOne({
                "object_hash": review.object_hash
            }, function (err, found) {

                if (err) {
                    return callback (err);
                }

                total += 1;

                if (!found) {

                    /*
                    * build query to either insert new docs, or
                    * update existing ones
                    * */
                    var query = {
                        updateOne: {
                            filter: {
                                "review_id": review.review_id
                            },
                            update: {
                                "$set": review
                            },
                            upsert:true
                        }
                    };

                    insert.push(query, function (err) {

                        if (err) {
                            return callback (err)
                        }

                        done();
                    });
                } else {

                    rejected += 1;
                    console.log("duplicate doc detected, omitting record", review.author);
                    done();

                }
            });
        });
    }, 100);


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

        processQueue.drain = function () {

            checkEndStream = setInterval(function () {
                console.log(total, rejected, processed);
                if (total && (total == rejected + processed)) {
                    console.log(2);
                    lib.kill(results.checkFileInDB, "ready");
                } else {
                    insert.drain = function() {
                        console.log(3);
                        lib.kill(results.checkFileInDB, "ready");
                    };
                }
            })

        };
    });

    rs.pipe(conv);
};

var readFileStats = function (callback) {

    /*
    * get file stats to keep track of any changes
    * */
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
                * file has been modified or didn't compete last
                * execution, update db record
                * */
                lib.updateTrackOps(stats, "processing", function (err, success) {

                    if (err) {
                        return callback(err);
                    }

                    callback(null, stats);

                });

            } else {

                /*
                * file was not modified, nothing to do
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

    /*
    * import to mongo starting point, processes 3 states
     * one after the other. passing the needed info from
     * one state to the next
    * */
    async.auto({
        readFileStats : readFileStats,
        checkFileInDB : ["readFileStats", checkFile],
        processData : ["checkFileInDB", processData]
    }, function (err) {

        /*
        * master error handler, all the errors will
        * be caught at this level
        * */

        if (err) {
            throw new Error(err);
        }

    });

} else {

    console.log("please enter a file name to proceed");
    process.exit();

}