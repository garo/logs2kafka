var extend = require('util')._extend;  
var inherits = require('util').inherits;  

var Statsd = require('statsd-client');

module.exports = StatsdHandler;

var defaultOptions = {  
  highWaterMark: 10,
  objectMode: true
};

function StatsdHandler(options) {  

  options = extend({}, options || {});
  options = extend(options, defaultOptions);

  this.options = options;
  console.log(options);

  this.statsd = new Statsd(options);

}

StatsdHandler.prototype.handle = handle;

function handle(event) {

  if (event.level) {
  	if (event.level == 'DEBUG') {
  		if (Math.floor((Math.random() * 31) + 1) == 1) {
	  		this.statsd.increment(event.service + '.app.log.DEBUG', 31);
  		}
  	} else {
  		this.statsd.increment(event.service + '.app.log.' + event.level);
  	}
  }
}
