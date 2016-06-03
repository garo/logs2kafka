var extend = require('util')._extend;  
var inherits = require('util').inherits;  
var Kafka = require('no-kafka');

var partitionIndices = {};

module.exports = KafkaHandler;

var defaultOptions = {  
  highWaterMark: 10,
  objectMode: true
};

function KafkaHandler(options, statsd) {  

  options = extend({}, options || {});
  options = extend(options, defaultOptions);

  this.options = options;
  this.maxRetries = 10;
  this.retryInterval = 200;
  this.topic = options.topic;
  this.statsd = statsd;

  options.partitioner = function(topic, partitions, message) {
  	// Pick simply random partition. A better way would be to use round robin with
  	// error checking.
  	return partitions[Math.floor((Math.random() * partitions.length))].partitionId;
  }

  this.producer = new Kafka.Producer(options);

  this.producer.init().then(function() {
  	console.log("Connected to kafka producer");
  });

}

KafkaHandler.prototype.handle = handle;
KafkaHandler.prototype.doSendMessage = doSendMessage;

function handle(event) {
	this.doSendMessage(this.topic, JSON.stringify(event), 0, function(){});
}

function doSendMessage(topic, message, retries, callback) {
  var self = this;

  this.producer.send({
  	topic: topic,
  	message: {
  		value: message
  	}
  }, {
  	batch: {
  		size: 1024,
  		maxWait: 200
  	}
  }).then(function (result) {
    if (result.error) {
      if (self.statsd) {
        self.statsd.increment('logs2kafka.sendFailed');
      }
	  console.log("Kafka promise", arguments);

      retries++;
      if (retries < self.maxRetries) {
        console.warn('Sending to Kafka failed, trying again:', result, topic);
        if (self.statsd) {
        	self.statsd.increment('logs2kafka.retrying');
        }
        setTimeout(doSendMessage.bind(self, topic, message, retries, callback), self.retryInterval);
      } else {
        console.error('Sending to Kafka failed, all partitions tried.', result);
        if (self.statsd) {
          self.statsd.increment('logs2kafka.couldNotSend');
        }
        callback(err);
      }
    } else {
      callback();
    }

  });
}
