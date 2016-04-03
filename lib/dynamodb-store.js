/*jslint node: true */
/* Copyright (c) 2012 Seamus D'Arcy */

 /*
  * Useful AWS Document references
  *
  * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html
  * http://aws.amazon.com/dynamodb/developer-resources/
  * https://aws.amazon.com/blogs/aws/dynamodb-local-for-desktop-development/
  * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html
  * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ExpressionPlaceholders.html
  * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html
  * http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html
  */

"use strict";
var aws = require('aws-sdk');
var async = require('async');
var _ = require('lodash');
var uuid = require('node-uuid');
var marshalItem = require('dynamodb-marshaler').marshalItem;
var unmarshalItem = require('dynamodb-marshaler').unmarshalItem;

var name = "dynamodb-store";
var ARRAY_TYPE = 'a';
var BOOL_TYPE = 'b';
var DATE_TYPE = 'd';
var OBJECT_TYPE = 'o';
var SENECA_TYPE_COLUMN = 'seneca';


module.exports = function(options) {
  var seneca = this;
  var desc;
  var minwait;
  var spec = null;
  var connection = null;

  options = seneca.util.deepextend({
    region: "us-west-1",
    apiVersion: 'latest'
  }, options);

  if( !options.endpoint && options.region ) {
    options.endpoint = "https://dynamodb." + options.region + ".amazonaws.com";
  } 


  /**
   * check and report error conditions seneca.fail will execute the callback
   * in the case of an error. Optionally attempt reconnect to the store depending
   * on error condition
   */
  function error(args, err, cb) {
   if( err ) {
     seneca.log.error('entity', err, {store:name});
     return true;
   }
   else return false;
 }



  /**
   * configure the store - create a new store specific connection object
   *
   * params:
   * spec - store specific configuration
   * cb - callback
   */
  function configure(specification, cb) {
    spec = specification;

    var conf = 'string' == typeof(spec) ? null : spec;
    aws.config.update({
      accessKeyId: conf.keyid,
      secretAccessKey: conf.secret,
      region: conf.region,
      apiVersion: conf.apiVersion
    });

    if (!conf.keyid) {
        return cb(new Error("configure: conf.keyid is required"));
    }
    if (!conf.secret) {
        return cb(new Error("configure: conf.secret is required"));
    }
    if (!conf.endpoint) {
        return cb(new Error("configure: conf.endpoint is required"));
    }

    connection = new aws.DynamoDB({
      endpoint: new aws.Endpoint(conf.endpoint)
    });
    seneca.log.debug('init', 'db open', spec);
    cb(null);
  }



  /**
   * the simple db store interface returned to seneca
   */
  var store = {
    name: name,

    /**
     * close the connection
     *
     * params
     * cmd - optional close command parameters
     * cb - callback
     */
    close: function(cmd, cb) {
      if (connection) {
        connection = null;
      }
      cb(null);
    },



    /**
     * save the data as specified in the entitiy block on the arguments object
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     */
    save: function(args, cb) {
      var ent  = args.ent;
      var update = !!ent.id;
      var table = tablename(ent);

      // Build SDK params
      var params = {
        TableName: table
      };

      // Manage entity ID
      if (!ent.id) {
        if (ent.id$) {
          ent.id = ent.id$;
        } else {
          ent.id = uuid();
        }
      }

      var entp = makeentp(ent);

      // using putItem for both insert and update, it's simpler
      if(update) {
        // id received - execute an update
        params.Item = marshalItem(entp);
        connection.putItem(params, function(err, res, cap) {
          if (!error(args, err, cb)) {
            seneca.log.info(args.tag$,'save/update', res);
            cb(null, ent);
          }
        });
      }
      else {
        // no id received - execute an insert
        params.Item = marshalItem(entp);
        connection.putItem(params, function(err, res) {
          if (!error(args, err, cb)) {
            seneca.log.info(args.tag$, 'save/insert', res);
            cb(null, ent);
          }
        });
      }
    },



    /**
     * load first matching item based on id
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     */
    load: function(args, cb) {
      var q = args.q;
      var qent = args.qent;
      var table = tablename(qent);
      var id;
      var params;

      if (q.id) { // fetch by primary key
        params = {
          TableName: table,
          Key: {
            id: {
              S: q.id
            }
          }
        };
        connection.getItem(params, function(err, res) {
          if (error(args, err, cb)) {
            cb(err);
          }
          seneca.log.info(args.tag$, 'load/get', res);
          if (!_.isEmpty(res) && !_.isEmpty(res.Item)) {
            cb(null, makeent(qent, unmarshalItem(res.Item)));
          } else {
            return cb(null, null);
          }
        });
      } else {
        // else q is a set of properties - Scan and return first result

        params = {
          TableName: table,
          ExclusiveStartKey: {
            id: {
              S: q.id
            }
          }
        };

        connection.scan(params, function(err, res) {
          if (error(args, err, cb)) {
            cb(err);
          }
          seneca.log.info(args.tag$, 'load/scan', res);
          if (!_.isEmpty(res)) {
            cb(null, makeent(qent, _.first(res.items)));
          } else {
            cb(null, null);
          }
        });
      }
    },



    /**
     * return a list of object based on the supplied query, if no query is supplied
     * then 'select * from ...'
     *
     * Notes: trivial implementation and unlikely to perform well due to list copy
     *        also only takes the first page of results from simple DB should in fact
     *        follow paging model
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     * all$ should only be included if list is called from remove
     *
     * a=1, b=2 simple
     * next paging is optional in simpledb
     * limit$ ->
     * use native$
     */
    list: function(args, cb) {

      var qent = args.qent;
      var q = args.q;
      var table = tablename(qent);

      var useGetItem = false;
      var itemID;

      var params = {
        TableName: table
      };

      // Required by DynamoDB for multy property scan filter
      var filterExpression = [];
      var expressionAttributeValues = {};

      if(!q.all$) {
        if(_.keys(q).length === 1 && q.id) {
          useGetItem = true;
          itemID = q.id;
        }
        else {
          // for each query property we need a filter expression (ie name=:name)
          // and a value placeholder (ie ExpressionAttributeValues[:name]:{S:"John"})
          // filters are joined with AND operator
          for (var param in q) {
             if(_.has(q, param) && !param.match(/\$$/)) {
                filterExpression.push("(" + param + "=:" + param + ")");
                switch (typeof q[param]) {
                  case "number":
                    expressionAttributeValues[":" + param] = {
                      N: String(q[param])
                    };
                    break;
                    case "boolean":
                      expressionAttributeValues[":" + param] = {
                        B: q[param]
                      };
                    break;
                  default:
                    // Defaulting all to String
                    expressionAttributeValues[":" + param] = {
                      S: String(q[param])
                    };
                }
             }
          }
        }
      }
      // Use getItem (faster) if ID is present
      if(useGetItem) {
        params.Key = {
          id: {
            S: itemID
          }
        };
        connection.getItem(params,function(err, res) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'list/get', res);
            cb(null, [makeent(qent, unmarshalItem(res.Item))]);
          }
        });
      }
      // Use scan
      else {

        // Add filter and attribute values if present
        if (filterExpression.length > 0) {
          params.FilterExpression = filterExpression.join(" AND ");
        }
        if (!_.isEmpty(expressionAttributeValues)) {
          params.ExpressionAttributeValues = expressionAttributeValues;
        }
        // dealing with DynamoDB pagination
        var morePages = true;
        var list = [];
        // repeat getPage function until there are pages to fetch
        async.until(function (){return (morePages === false);}, function getPage(next){
          connection.scan(params, function(err, res) {
            if (!error(args, err, cb)) {
              var items = res.Items.map(unmarshalItem);
              items.forEach(function(item) {
                var ent = makeent(qent, item);
                list.push(ent);
              });
              // if LastEvaluatedKey field is not empty there are more items to fetch
              if (res.LastEvaluatedKey) {
                // fetch the next page starting at LastEvaluatedKey
                params.ExclusiveStartKey = res.LastEvaluatedKey;
              } else {
                // this is the last page
                delete params.ExclusiveStartKey;
                morePages = false;
              }

              seneca.log(args.tag$, 'list/page', res);

              // next page or end, depending on condition
              next();
            }
          });
        // all pages fetched, prepare and return result
        }, function end(err){
            // we have all the totalItems now
            seneca.log(args.tag$, 'list/scan', list);
            cb(null, list);
        });
      }
    },



    /**
     * delete an item - fix this
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     * { 'all$': true }
     */
    remove: function(args, cb) {

      var table = tablename(args.qent);

      store.list(args, function(err, res) {
        if (!error(args, err, cb)) {
          var deletes = [];
          res.forEach(function(entity) {
            deletes.push(deleteItem(connection, table, entity.id));
          });
          async.parallel(deletes, function(err, res) {
            if (!error(args, err, cb)) {
              cb(null);
            }
          });
        }
      });
    },



    /**
     * return the underlying native connection object
     */
    native: function(args, cb) {

      var ent = args.ent;

      // provide access to the underlying driver
      cb(null, connection);
    }
  };



  /**
   * initialization
   */
  var meta = seneca.store.init(seneca, options, store);
  desc = meta.desc;
  seneca.add({init:store.name,tag:meta.tag}, function(args,done) {
    configure(options, function(err) {
      if (err) {
        return seneca.die('entity/configure', err, {
          store: store.name,
          desc: desc
        });
      }
      else done();
    });
  });
  return { name:store.name, tag:meta.tag };
};



var tablename = function(entity) {
  var canon = entity.canon$({object: true});
  return(canon.base ? canon.base + '_' : '') + canon.name;
};



/* create a persistable entity from the entity object (serialize unsupported types) */
var makeentp = function(ent) {
  var entp = {};
  var fields = ent.fields$();
  var type = {};

  fields.forEach(function(field) {
    if(_.isArray( ent[field])) {
      type[field] = ARRAY_TYPE;
    } else if(_.isBoolean(ent[field])) {
      type[field] = BOOL_TYPE;
    } else if(_.isDate(ent[field])) {
      type[field] = DATE_TYPE;
    } else if(!_.isArray(ent[field]) && _.isObject(ent[field])) {
      type[field] = OBJECT_TYPE;
    }

    if(_.isBoolean(ent[field]) || _.isObject(ent[field])) {
      entp[field] = JSON.stringify(ent[field]);
    } else {
      entp[field] = ent[field];
    }
  });

  if(!_.isEmpty(type)) {
    entp[SENECA_TYPE_COLUMN] = JSON.stringify(type);
  }
  return entp;
};



/* create a new entity from returned persistent entity (deserialize unsupported types) */
var makeent = function(ent, row) {
  var entp = {};
  var fields = _.keys(row);
  var senecatype = {};
  var conversion = {};

  if(!_.isUndefined(row[SENECA_TYPE_COLUMN]) && !_.isNull(row[SENECA_TYPE_COLUMN])) {
    senecatype = JSON.parse(row[SENECA_TYPE_COLUMN]);
  }

  conversion[ARRAY_TYPE] = conversion[OBJECT_TYPE] = function(field) { return JSON.parse(field); };
  conversion[DATE_TYPE] = function(field) { return new Date(JSON.parse(field)); };
  conversion[BOOL_TYPE] = function(field) { return Boolean(JSON.parse(field)); };

  fields.forEach(function(field) {
    if(field != SENECA_TYPE_COLUMN) {
      var convert = conversion[senecatype[field]];
      if (convert) {
        entp[field] = convert(row[field]);
      } else {
        entp[field] = row[field];
      }
    }
  });
  return ent.make$(entp);
};



/* returns a delete request for the specified table and item id */
var deleteItem = function(connection, table, id) {
  return function(cb) {
    var params = {
      TableName: table,
      Key: {
        id: {S: id}
      },
      ReturnValues: "ALL_OLD"
    };
    connection.deleteItem(params, function(err, res, cap) {
      if(err) {
        cb(err);
      } else {
        cb(null, res);
      }
    });
  };
};
