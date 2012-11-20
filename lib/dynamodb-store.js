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
    return seneca.fail({
      code: 'save', 
      store: self.name
    }, cb);
  };

  /** load the first matching entity */
  self.load$ = function(args, cb) {
    return seneca.fail({
      code: 'load', 
      store: self.name
    }, cb);
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