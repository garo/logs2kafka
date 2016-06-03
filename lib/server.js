var net = require('net');  
var JSONDuplexStream = require('json-duplex-stream');

var MessageProcessor = require('./message-processor');
var StatsdHandler = require('./statsd-handler');
var KafkaHandler = require('./kafka-handler');


module.exports = Server;

function Server(options) {  
  var self = this;

  this.server = net.createServer();

  this.statsdHandler = new StatsdHandler(options.statsd);
  this.kafkaHandler = new KafkaHandler(options.kafka, this.statsdHandler.statsd);


  this.server.on('connection', function(conn) {
    self.handleConnection(conn);
  });  
  this.server.listen(options.port, function() {  
    console.log('server listening on %j', self.server.address());
  });

}

Server.prototype.handleConnection = function(conn) {  
  var s = new JSONDuplexStream();

  var processor = new MessageProcessor({}, [this.statsdHandler, this.kafkaHandler]);
  conn.pipe(s.in).pipe(processor).pipe(s.out).pipe(conn);

  s.in.on('error', onProtocolError);
  s.out.on('error', onProtocolError);
  conn.on('error', onConnError);

  function onProtocolError(err) {
    console.error(err);
    conn.end('protocol error:' + err.message);
  }
};

function onConnError(err) {  
  console.error('connection error:', err.stack);
}

Server.prototype.close = function() {
  this.server.close();
};



