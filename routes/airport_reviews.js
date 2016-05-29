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
    db.bind('reviews');

    console.log(env)
    db.reviews.find(
        {
            "airport_name": airport
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

        console.log(config);
        if (!results.length) {
            return next(new Error(config.errors["719"]));
        }
        res.json({
            "success": true,
            "results": results
        });

    });
};