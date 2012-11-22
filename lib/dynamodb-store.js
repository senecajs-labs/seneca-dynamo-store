/* Copyright (c) 2012 Seamus D'Arcy */

var common    = require('seneca/lib/common');
var Store     = require('seneca').Store;
var dynamodb  = require('dynamodb');

var eyes      = common.eyes; // Used for development only
var _         = common._;
var uuid      = common.uuid;

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

    if( !ent.id ) {
      ent.id = uuid();
    }

    var entp = {};
    var fields = ent.fields$();
    fields.forEach(function(field) {
      entp[field] = ent[field];
    });

    if(update) {
      // id received - execute an update

      var updates = {};

      for(var e in entp) {
        if(entp.hasOwnProperty(e) && 'function' != typeof(entp[e]) && e != 'id') {
          updates[e] = {value: entp[e], action: 'PUT'};
        }
      }

      self.connection.updateItem(tablename(ent), ent.id, null, updates, {}, function(err, res, cap) {
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
      self.connection.putItem(tablename(ent), entp, {}, function(err, res, cap) {
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
        return seneca.fail({code:'load', tag: args.tag$, store: self.name, query: query, error:err}, cb);
      }
      else {
        var fent = qent.make$(res);
        seneca.log(args.tag$, 'load', res);
        cb(null, fent);
      }
    });
  };

  /** load all matching entities */
  self.list$ = function(args, cb) {
    return seneca.fail({
      code: 'list', 
      store: self.name
    }, cb);
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

  var tablename = function(entity) {
    var canon = entity.canon$({object: true});
    return(canon.base ? canon.base + '_' : '') + canon.name;
  }

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