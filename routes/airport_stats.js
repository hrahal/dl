/**
 * Created by hrahhal on 5/28/16.
 */
"use strict";

var env = require('express')().get("env");
var mongo = require('mongoskin');
var config = require("../config");
var db = mongo.db("mongodb://" + config[env].mongo.host + ":" +
    config[env].mongo.port + "/airport", {native_parser:true});



/* GET airport stats. */

module.exports = function(req, res, next) {

    var airport = req.params.airport;
    db.bind('reviews');

    db.reviews.aggregate([
        {
            "$match": {
                "airport_name": airport
            }
        },
        {
            "$group": {
                "_id": {
                    "airport_name": "$airport_name"
                },
                "reviews_count": {
                    "$sum": 1
                },
                "average_rating": {
                    "$avg": "$overall_rating"
                },
                "recommended_count": {
                    "$sum": "$recommended"
                }
            }
        },
        {
            "$sort": {
                "count": -1
            }
        }
    ], function (err, results) {

        db.close();

        if (err) {
            return next(new Error(err));
        }

        if (!results.length) {
            return (next(new Error(config.errors["719"])));
        }
        res.json({
            "success": true,
            "results": results[0]
        });
        
    });
};