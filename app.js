var Server = require('./lib/server');

var server = new Server({
	statsd : {
	    host : (process.env.STATSD_HOST || "localhost"),
	    port : (process.env.STATSD_PORT || 8125),
	    debug : false		
	},
	port : (process.env.TCP_LISTEN_PORT || 8061),
	kafka : {
		connectionString : process.env.KAFKA_CONNECTION_STRING,
		topic : (process.env.TOPIC || "docker-stream")
	}
  });