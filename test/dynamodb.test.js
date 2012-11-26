/* Copyright (c) 2012 Seamus D'Arcy */

// These tests assume the existance of tables 'foo' and 'moon_bar' with hash key 'id'.

var seneca   = require('seneca');
var shared   = require('seneca/test/store/shared');

var config = {
  log:'print'
};

var si = seneca(config);

var senecaDynamoDBStore = require('../lib/dynamodb-store');
var senecaDynamoDBStoreOpts = {
  accessKeyId: 'ACCESSKEYID',
  secretAccessKey: 'SECRETACCESSKEY',
  endpoint: 'ENDPOINT'
};

si.use(senecaDynamoDBStore, senecaDynamoDBStoreOpts);

si.__testcount = 0;
var testcount = 0;

module.exports = {
  basictest: (testcount++, shared.basictest(si)),
  extratest: (testcount++, extratest(si)),
  closetest: shared.closetest(si, testcount)
};

function extratest(si) {
  console.log('EXTRA');
  si.__testcount++;
}