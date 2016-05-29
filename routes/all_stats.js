/**
 * Created by hrahhal on 5/28/16.
 */
"use strict";

var env = require('express')().get("env");
var mongo = require('mongoskin');
var config = require("../config");
var db = mongo.db("mongodb://" + config[env].mongo.host + ":" +
    config[env].mongo.port + "/airport", {native_parser:true});


/* GET home page. */

module.exports = function(req, res, next) {
    
    db.bind('reviews');

    db.reviews.aggregate([
        {
            "$group": {
                "_id": {
                    "airport_name": "$airport_name"
                },
                "count": {
                    "$sum": 1
                }
            }
        },
        {
            "$sort": {
                count: -1
            }
        }
    ], function (err, results) {

        db.close();
        
        if (err) {
            return next(new Error(err));
        }

        res.json({
            "success": true,
            "results": results
        });

    });
};