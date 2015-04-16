seneca-dynamodb
===============

seneca-dynamodb is an [Amazon DynamoDB][dynamodb] database driver for the [Seneca][seneca] MVP toolkit.

Usage:

    var seneca = require('seneca');
    var senecaDynamoDBStore = require('seneca-dynamodb');

    var senecaConfig = {}
    var senecaDynamoDBStoreOpts = {
      keyid: 'ACCESSKEYID',
      secret: 'SECRETACCESSKEY',
      endpoint: 'ENDPOINT'
    };

    ...

    var si = seneca(senecaConfig);
    si.use(senecaDynamoDBStore, senecaDynamoDBStoreOpts);
    si.ready(function() {
      var product = si.make('product');
      ...
    });
    ...

[dynamodb]: http://aws.amazon.com/dynamodb
[seneca]: http://senecajs.org/
