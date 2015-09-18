/*jslint node: true */
"use strict";

var aws = require('aws-sdk');
var async = require('async');
var keys = require('../test/keys.mine.js');

console.info("Cleaning database after tests...\n");

var dynamoDBConfiguration = {
    accessKeyId: keys.id,
    secretAccessKey: keys.secret,
    region: keys.region
};
var databaseConfig = {"endpoint": new aws.Endpoint(keys.endpoint)};
aws.config.update(dynamoDBConfiguration);
var ddb = new aws.DynamoDB(databaseConfig);

ddb.tableExists = function(params, callback) {
    ddb.describeTable(params, function(err, res) {
        if (err) {
            if ("ResourceNotFoundException" === err.code) {
                return callback(null, false);
            }
            return callback(err);
        }
        return callback(null, true);
    });
};

async.series([
    function deleteTableFoo(next){
        ddb.tableExists({TableName: "foo"}, function(err, res) {
            if (err) {
                console.log(err);
                return next(err);
            }
            if (true === res) {
                ddb.deleteTable({TableName: 'foo'}, function (error) {
                    if (error) {
                        console.log("Error: ", error, error.stack);
                        return next(error);
                    }
                    // console.log("Table ", 'foo', " Dropped!");
                    return next(null, "Table foo dropped");
                });
            } else {
                // console.info("Table foo does not exist");
                return next(null, "Table foo does not exist");
            }
        });
    },
    function deleteTableMoonBar(next){
        ddb.tableExists({TableName: "moon_bar"}, function(err, res) {
            if (err) {
                console.log(err);
                return next(err);
            }
            if (true === res) {
                ddb.deleteTable({TableName: 'moon_bar'}, function (error) {
                    if (error) {
                        console.log("Error: ", error, error.stack);
                        return next(error);
                    }
                    // console.log("Table ", 'moon_bar', " Dropped!");
                    return next(null, "Table moon_bar dropped");
                });
            } else {
                // console.info("Table moon_bar does not exist");
                return next(null, "Table moon_bar does not exist");
            }
        });
    },
], function summary(err, results) {
    if (err) {
        console.error(err);
    }
    if (results) {
        results.forEach(function (item){
            console.info(" - " + item);
        });
        console.info("\n...all done!");
    }
});
