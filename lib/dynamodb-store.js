/* Copyright (c) 2012 Seamus D'Arcy */

/*
 * TODO
 * - load$ - support a set of properties as input (use Scan instead of GetItem in this case)
 * - load$ - implement sort
 * - Handle API paging e.g. LastEvaluatedKey where number of scanned items exceeds 1MB
 * - list$ - handle query string as input
 * - Serialization of unsupported types: the basis for makeentp() has been moved to seneca RelationalStore but Boolean is not handled
 */

var Store     = require('seneca').Store;
var dynamodb  = require('dynamodb');
var async     = require('async');
var _         = require('underscore');
var uuid      = require('node-uuid');

var ARRAY_TYPE = 'a';
var BOOL_TYPE = 'b';
var DATE_TYPE = 'd';
var OBJECT_TYPE = 'o';
var SENECA_TYPE_COLUMN = 'seneca';

function DynamoDBStore() {

  var self = new Store();
  var parent = self.parent();

  var seneca;

  self.name = 'seneca-dynamodb';

  /* called by seneca to initialise plugin */
  self.init = function(si, opts, cb) {
    parent.init(si, opts, function() {

      // keep a reference to the seneca instance
      seneca = si;

      self.configure(opts, function(err) {
        if(err) {
          return seneca.fail({code: 'entity', store: self.name, error: err}, cb);
        }
        else cb();
      });
    });
  };

  /* create or update an entity */
  self.save$ = function(args, cb) {
    // entity to save
    var ent  = args.ent;
    var update = !!ent.id;
    var table = tablename(ent);

    if( !ent.id ) {
      ent.id = uuid();
    }

    var entp = makeentp(ent);

    if(update) {
      // id received - execute an update

      var updates = {};

      for(var e in entp) {
        if(entp.hasOwnProperty(e) && 'function' != typeof(entp[e]) && e != 'id') {
          updates[e] = {value: entp[e], action: 'PUT'};
        }
      }

      self.connection.updateItem(table, ent.id, null, updates, {}, function(err, res, cap) {
        if(err) {
          return seneca.fail({code: 'save/update', tag: args.tag$, store: self.name, error: err }, cb);
        }
        else {
          seneca.log(args.tag$,'save/update', res);
          cb(null, ent);
        }
      });
    }
    else {
      // no id received - execute an insert
      self.connection.putItem(table, entp, {}, function(err, res, cap) {
        if(err) {
          return seneca.fail({code: 'save/insert', tag: args.tag$, store: self.name, error: err }, cb);
        }
        else {
          seneca.log(args.tag$, 'save/insert', res);
          cb(null, ent);
        }
      });
    }
  };

  /* load the first matching entity */
  self.load$ = function(args, cb) {

    var q = _.clone(args.q);
    var qent = args.qent;

    self.connection.getItem(tablename(qent), qent.id, null, {}, function(err, res, cap) {
      if(err) {
        return seneca.fail({code:'load', tag: args.tag$, store: self.name, error: err}, cb);
      }
      else {
        var fent = makeent(qent, res);
        seneca.log(args.tag$, 'load', res);
        cb(null, fent);
      }
    });
  };

  /* load all matching entities */
  self.list$ = function(args, cb) {

    var qent = args.qent;
    var q = args.q;
    var table = tablename(qent);

    var options = {};
    var filter = {};
    var useGetItem = false;
    var itemID;

    if(!q.all$) { // all$ should only be included if list is called from remove
      if(_.keys(q).length === 1 && q.id) {
        useGetItem = true;
        itemID = q.id;
      } else {
        for (var param in q) {
           if(_.has(q, param)) {
              filter[param] = {eq: q[param]};
           }
        }
        options.filter = filter;
      }
    }

    if(useGetItem) {
      self.connection.getItem(table, itemID, null, {}, function(err, res, cap) {
        if(err) {
          return seneca.fail({code:'list/get', tag: args.tag$, store: self.name, error: err}, cb);
        }
        else {
          seneca.log(args.tag$, 'list/get', res);
          cb(null, [makeent(qent, res)]);
        }
      });
    } else {
      self.connection.scan(table, options, function(err, res) {
        if(err) {
          return seneca.fail({code:'list/scan', tag: args.tag$, store: self.name, error: err}, cb);
        } else {
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
  };

  /* remove all matching entities */
  self.remove$ = function(args, cb) {
    var table = tablename(args.qent);

    // list entities matching args
    self.list$(args, function(err, res) {
      if(err) {
        return seneca.fail({code:'remove/list', tag: args.tag$, store: self.name, error: err}, cb);
      } else {
        // create delete request request for each item
        var deletes = [];
        res.forEach(function(entity) {
          deletes.push(deleteItem(table, entity.id));
        });

        async.parallel(deletes, function(err, res) {
          if(err) {
            return seneca.fail({code:'remove/delete', tag: args.tag$, store: self.name, error: err}, cb);
          } else {
            cb(null);
          }
        });
      }
    });
  };

  /* close connection to data store - called during shutdown */
  self.close$ = function(args, cb) {
    seneca.log(args.tag$, 'close');
    cb();
  };

  self.configure = function(spec, cb) {
    self.spec = spec;

    var conf = 'string' == typeof(spec) ? null : spec;

    self.connection = dynamodb.ddb({
      accessKeyId: conf.accessKeyId,
      secretAccessKey: conf.secretAccessKey,
      endpoint: conf.endpoint
    });

    seneca.log({tag$: 'init'}, 'db open');
    cb(null, self);
  };

  /* returns a delete request for the specified table and item id */
  var deleteItem = function(table, id) {
    return function(cb) {
      self.connection.deleteItem(table, id, null, {returnValues: 'ALL_OLD'}, function(err, res, cap) {
        if(err) {
          cb(err);
        } else {
          cb(null, res);
        }
      });
    };
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

  var tablename = function(entity) {
    var canon = entity.canon$({object: true});
    return(canon.base ? canon.base + '_' : '') + canon.name;
  };

  return self;
}

module.exports = new DynamoDBStore();