var Server = require('../lib/server');
var should = require('should');
var sinon = require('sinon');
var net = require('net');

describe('server', function() {
  var sandbox;

  before(function() {
    sandbox = sinon.sandbox.create();
  });

  afterEach(function() {
    sandbox.restore();
  });
  
  beforeEach(function() {

  });

  it('should be able to listen incoming messages', function(done) {
    var server = new Server({statsdOptions:{}, port: 8061});

    var connected = false;
    var received = null;

    var client = new net.Socket();
	client.connect(8061, '127.0.0.1', function() {
		connected = true;
		client.write(JSON.stringify({"service":"test"}) + "\n");
		client.destroy();
		server.close();
		done();
	});

	client.on('close', function() {
		console.log('Connection closed');
	});

  });
});