/*jslint node: true */
"use strict";

var aws = require('aws-sdk');
var async = require('async');
var keys = require('../test/keys.mine.js');

console.info("Preparing database for tests...\n");

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

var tableFoo = {
    AttributeDefinitions: [ // Defining Primary Key
        {
          AttributeName: 'order_id',
          AttributeType: 'N'
        }
        // Define Secondary key here.
      ],
      KeySchema: [ // Defining Key Type Here.
        {
          AttributeName: 'order_id',
          KeyType: 'HASH'
        }
        // Define Secondary Key Type Here.
      ],
      // Define read per second and write per second here.
      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 5
      },
      TableName: 'foo' // table Name
};


var tableMoonBar = {
    AttributeDefinitions: [ // Defining Primary Key
        {
          AttributeName: 'order_id',
          AttributeType: 'N'
        }
        // Define Secondary key here.
      ],
      KeySchema: [ // Defining Key Type Here.
        {
          AttributeName: 'order_id',
          KeyType: 'HASH'
        }
        // Define Secondary Key Type Here.
      ],
      // Define read per second and write per second here.
      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 5
      },
      TableName: 'moon_bar' // table Name
};

async.series([
    function createTableFoo(next){
        ddb.tableExists({TableName: "foo"}, function(err, res) {
            if (err) {
                console.log(err);
                return next(err);
            }
            if (true === res) {
                return next(null, "Table foo already exists");
            } else {
                return ddb.createTable(tableFoo, function(err, details) {
                    if (err) {
                        console.log(err);
                        return next(err);
                    }
                    next(null, "Table foo created successfully");
                });
            }
        });
    },
    function createTableMoonBar(next){
        ddb.tableExists({TableName: "moon_bar"}, function(err, res) {
            if (err) {
                console.log(err);
                return next(err);
            }
            if (true === res) {
                return next(null, "Table moon_bar already exists");
            } else {
                return ddb.createTable(tableMoonBar, function(err, details) {
                    if (err) {
                        console.log(err);
                        return next(err);
                    }
                    next(null, "Table moon_bar created successfully");
                });
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
        console.info("\n...ready!");
    }
});
