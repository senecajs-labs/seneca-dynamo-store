/* Copyright (c) 2012 Seamus D'Arcy */

var common    = require('seneca/lib/common');
var Store     = require('seneca').Store;
var dynamodb  = require('dynamodb');

var eyes      = common.eyes; // Used for development only
var _         = common._;
var uuid      = common.uuid;

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

  /** called by seneca to initialise plugin */
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

  /** create or update an entity */
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
          return seneca.fail({code: 'save/update', tag: args.tag$, store: self.name, fields: fields, error: err }, cb);
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
          return seneca.fail({code: 'save/insert', tag: args.tag$, store: self.name, fields: fields, error: err }, cb);
        }
        else {
          seneca.log(args.tag$, 'save/insert', res);
          cb(null, ent);
        }
      });
    }
  };

  /** load the first matching entity */
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

  /** load all matching entities */
  self.list$ = function(args, cb) {

    var qent = args.qent;
    var q = args.q;

    var options = {};
    var filter = {};

    for (var param in q) {
       if(q.hasOwnProperty(param)) {
          filter[param] = {eq: q[param]};
       }
    }

    if(!_.isEmpty(filter)) {
      options.filter = filter;
    }

    // TODO: use getItem instead of scan if q contains only id

    self.connection.scan(tablename(qent), options, function(err, res) {
      if(err) {
        return seneca.fail({code:'list', tag: args.tag$, store: self.name, error: err}, cb);
      } else {
        var list = [];
        res.items.forEach(function(item) {
          var ent = makeent(qent, item);
          list.push(ent);
        });
        cb(null, list);
      }
    });
  };

  /** remove all matching entities */
  self.remove$ = function(args, cb) {
    return seneca.fail({
      code: 'remove',
      store: self.name
    }, cb);
  };

  /** close connection to data store - called during shutdown */
  self.close$ = function(args, cb) {
    return seneca.fail({
      code: 'connection/end',
      store: self.name
    }, cb);
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

  function error(args, err, cb) {
    if(err) {
      if(!err.fatal) {
        return false;
      }

      seneca.log(args.tag$, 'error: ' + err);
      seneca.fail({code: 'entity/error', store: self.name }, cb);
      return true;
    }

    return false;
  }

  return self;
}

module.exports = new DynamoDBStore();