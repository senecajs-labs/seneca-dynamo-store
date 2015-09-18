/*jslint node: true */
/* Copyright (c) 2012 Seamus D'Arcy */
/*
 * TODO
 * - list$, load$ - implement basic sort
 * - refactor - extract GetItem and Scan, used by list$ and load$
 * - list$, load$ - handle multiple sort parameters
 * - Handle API paging e.g. LastEvaluatedKey where number of scanned items exceeds 1MB
 * - list$ - handle query string as input
 * - Serialization of unsupported types: the basis for makeentp() has been moved to seneca RelationalStore but Boolean is not handled
 */

"use strict";
var aws = require('aws-sdk');
var assert = require("assert");
var Store = require('seneca').Store;
var async = require('async');
var _ = require('lodash');
var uuid = require('node-uuid');

var NAME = "dynamodb-store";
var ARRAY_TYPE = 'a';
var BOOL_TYPE = 'b';
var DATE_TYPE = 'd';
var OBJECT_TYPE = 'o';
var SENECA_TYPE_COLUMN = 'seneca';


module.exports = function(opts) {
  var seneca = this;
  var desc;
  var minwait;
  var spec = null;
  var connection = null;



  /**
   * check and report error conditions seneca.fail will execute the callback
   * in the case of an error. Optionally attempt reconnect to the store depending
   * on error condition
   */
  function error(args, err, cb) {
    if (err) {
      seneca.log.debug('error: ' + err);
      seneca.fail({code:'entity/error', store: NAME}, cb);
    }
    return err;
  }



  /**
   * configure the store - create a new store specific connection object
   *
   * params:
   * spec - store specific configuration
   * cb - callback
   */
  function configure(specification, cb) {
    assert(specification);
    assert(cb);
    spec = specification;


    debugger;
    var conf = 'string' == typeof(spec) ? null : spec;
    connection = dynamodb.ddb({accessKeyId: conf.keyid,
                               secretAccessKey: conf.secret});
//                               endpoint: conf.endpoint});
    seneca.log.debug('init', 'db open', spec);
    cb(null);
  }



  /**
   * the simple db store interface returned to seneca
   */
  var store = {
    name: NAME,


    /**
     * close the connection
     *
     * params
     * cmd - optional close command parameters
     * cb - callback
     */
    close: function(cmd, cb) {
      assert(cb);
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
      assert(args);
      assert(cb);
      assert(args.ent);
      
          debugger;

      var ent  = args.ent;
      var update = !!ent.id;
      var table = tablename(ent);

      if (!ent.id) {
        ent.id = uuid();
      }

      var entp = makeentp(ent);

      if(update) {
        // id received - execute an update

        var updates = {};

        for(var e in entp) {
          if(_.has(entp, e) && !_.isFunction(entp[e]) && e != 'id') {
            updates[e] = {value: entp[e], action: 'PUT'};
          }
        }

        connection.updateItem(table, ent.id, null, updates, {}, function(err, res, cap) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$,'save/update', res);
            cb(null, ent);
          }
        });
      }
      else {
        // no id received - execute an insert
        connection.putItem(table, entp, {}, function(err, res, cap) {
          debugger;
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'save/insert', res);
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
      assert(args);
      assert(cb);
      assert(args.qent);
      assert(args.q);

      var q = _.clone(args.q);
      var qent = args.qent;
      var table = tablename(qent);
      var id;

      if(_.isString(q)) {
        connection.getItem(table, q, null, {}, function(err, res, cap) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'load/get', res);
            cb(null, makeent(qent, res));
          }
        });
      }
      else { // else q is a set of properties - Scan and return first result
        var options = {};
        var filter = {};
        for (var param in q) {
           if(_.has(q, param) && !param.match(/\$$/)) {
              filter[param] = {eq: q[param]};
           }
        }
        options.filter = filter;

        connection.scan(table, options, function(err, res) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'load/scan', res);
            cb(null, makeent(qent, _.first(res.items)));
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
      assert(args);
      assert(cb);
      assert(args.qent);
      assert(args.q);

      var qent = args.qent;
      var q = args.q;
      var table = tablename(qent);

      var options = {};
      var filter = {};
      var useGetItem = false;
      var itemID;

      if(!q.all$) {
        if(_.keys(q).length === 1 && q.id) {
          useGetItem = true;
          itemID = q.id;
        }
        else {
          for (var param in q) {
             if(_.has(q, param) && !param.match(/\$$/)) {
                filter[param] = {eq: q[param]};
             }
          }
          options.filter = filter;
        }
      }
      if(useGetItem) {
        connection.getItem(table, itemID, null, {}, function(err, res, cap) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'list/get', res);
            cb(null, [makeent(qent, res)]);
          }
        });
      }
      else {
        connection.scan(table, options, function(err, res) {
          if (!error(args, err, cb)) {
            var list = [];
            res.items.forEach(function(item) {
              var ent = makeent(qent, item);
              list.push(ent);
            });
            seneca.log(args.tag$, 'list/scan', res);
            cb(null, list);
          }
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
      assert(args);
      assert(cb);
      assert(args.qent);
      assert(args.q);

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
      assert(args);
      assert(cb);
      assert(args.ent);

      var ent = args.ent;

      // provide access to the underlying driver
      // cb(null, db);
    }
  };



  /**
   * initialization
   */
  var meta = seneca.store.init(seneca, opts, store);
  desc = meta.desc;
  seneca.add({init:store.name,tag:meta.tag}, function(args,done) {
    configure(opts, function(err) {
      if (err) {
        return seneca.fail({code:'entity/configure', store:store.name, error:err, desc:desc}, done);
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
    connection.deleteItem(table, id, null, {returnValues: 'ALL_OLD'}, function(err, res, cap) {
      if(err) {
        cb(err);
      } else {
        cb(null, res);
      }
    });
  };
};

