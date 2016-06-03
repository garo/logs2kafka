logs2kafka
==========

Simple tool to listen for incoming log messages and send them further to Kafka.

The tool is designed to produce JSON based log messages which have at least the following fields:

	{
	  "ts" : "2016-06-03T11:55:55.391Z",
	  "service" : "my-unique-service-name",
	  "host" : "hostname.company.com"
	}

In addition if a SERVER_IP env variable is provided a "server_ip" field is added with this value.

Each message is sent to a kafka cluster (KAFKA_CONNECTION_STRING env variable, eg. "localhost:9042") into a topic (TOPIC env variable), into a random partition inside this topic. If the partition is not available then another partition is picked up by random.

Sending logs to this relay
--------------------------

This service lists on a TCP port (env TCP_LISTEN_PORT, default 8061) for incoming JSON encoded log messages. Each log message must end with a new line ("\n") and a JSON log message itself can't have new line characters.

The service will add an ISO-8601 formatted timestamp to "ts" field and the machine hostname into the "host" field if either of them are missing.

The service will refuse to forward a message if it doesn't have "service" field set.

Statsd metrics
--------------

Service will also send some statsd metrics into a statsd server (STATSD_HOST and STATSD_PORT env variables):

 - If a message contains "level" attribute, then a counter with name `service + ".app.log." + level` is emitted.

 - `logs2kafka.sendFailed` is incremented in all Kafka errors/warnings.

 - `logs2kafka.retrying` is incremented each time a message is being tried into another partition (so first try does not count).

 - `logs2kafka.couldNotSend` is incremented if message failed completely and was lost.