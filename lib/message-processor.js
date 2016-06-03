var extend = require('util')._extend;  
var inherits = require('util').inherits;  
var Transform = require('stream').Transform;
var os = require('os');

module.exports = MessageProcessor;

inherits(MessageProcessor, Transform);

var defaultOptions = {  
  highWaterMark: 10,
  objectMode: true
};

var thehost = os.hostname();

var theip = null;
if (process.env.SERVER_IP) {
  theip = process.env.SERVER_IP;
}


function MessageProcessor(options, processors) {  

  this.processors = processors;

  options = extend({}, options || {});
  options = extend(options, defaultOptions);

  this.valid_levels = ['DEBUG', 'INFO', 'WARN', 'ERROR'];

  Transform.call(this, options);

}

/// _transform

MessageProcessor.prototype._transform = _transform;

function _transform(event, encoding, callback) {
  if (!event.service) {
    return handleError(new Error('log json blob doesn\'t have an `service` field'));
  }

  if (!event.ts) {
    event.ts = new Date().toISOString();
  }

  if (event.level && !this.valid_levels.indexOf(event.level)) {
    event.level = "UNKNOWN";
  }

  if (!event.host) {
    event.host = thehost;
  }

  if (!event.server_ip && theip) {
    event.server_ip = theip;
  }



  function handleError(err) {
    var reply = {
      id: event.id,
      success: false,
      error: err.message
    };

    callback(null, reply);
  }  

  for (var i = 0; i < this.processors.length; i++) {
    this.processors[i].handle(event);
  }

  callback(null);
}
