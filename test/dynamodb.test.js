/*jslint node: true */
/*global describe:true, it:true, mocha:true*/
/* Copyright (c) 2012 Seamus D'Arcy */
/* These tests assume the existance of tables 'foo' and 'moon_bar' with hash key 'id'
 *
 * Run with
 * npm test
 */

"use strict";

var seneca = require('seneca');
var shared = require('seneca-store-test');
var keys = {
  id: process.env.AWS_KEY_ID || "EmptyKey",
  secret: process.env.AWS_KEY_SECRET || "EmptySecret",
  endpoint: process.env.AWS_KEY_ENDPOINT || "http://localhost:8000",
  region: process.env.AWS_KEY_REGION || "eu-west-1"
};

var lab = exports.lab = require('lab').script();
var describe = lab.describe;
var it = lab.it;

var si = seneca();

si.use(require('..'), {keyid: keys.id, secret: keys.secret, endpoint: keys.endpoint});

si.__testcount = 0;
var testcount = 0;


describe('DynamoDB', function(){
  shared.basictest({seneca:si, script:lab});
});
