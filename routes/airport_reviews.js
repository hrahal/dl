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

    var airport = req.params.airport;
    var overall_rating_min = parseInt(req.query.or_min) || 0;

    db.bind('reviews');

    db.reviews.find(
        {
            "$and": [
                {
                    "airport_name": airport
                },
                {
                    "overall_rating": {
                        "$gte": overall_rating_min
                    }
                }
            ]
        },
        {
            "overall_rating": 1,
            "recommendation": 1,
            "date": 1,
            "author_country": 1,
            "content": 1
        },
        {
            "$sort": {
                "date": -1
            }
        }
    ).toArray(function (err, results) {

        db.close();

        if (err) {
            return next(new Error(err));
        }

        if (!results.length) {
            return next(new Error(config.errors["719"]));
        }
        res.json({
            "success": true,
            "results": results
        });

    });
};