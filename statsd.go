package main

//import "github.com/cactus/go-statsd-client/statsd"
import "time"
import "fmt"

type StatisticsSender interface {
	Inc(string, int64, float32) error
	Dec(string, int64, float32) error
	Gauge(string, int64, float32) error
	GaugeDelta(string, int64, float32) error
	Timing(string, int64, float32) error
	TimingDuration(string, time.Duration, float32) error
	Set(string, string, float32) error
	SetInt(string, int64, float32) error
	Raw(string, string, float32) error
}

func SendStatsdMetricsFromMessage(statsd StatisticsSender, m *Message) (error) {

	service, ok := m.Container.Path("service").Data().(string)
	if !ok {
		statsd.Inc("logs2kafka.unknown_service", 1, 1)
		return nil
	}

	level, ok := m.Container.Path("level").Data().(string)
	if ok {
		if level == "DEBUG" {
			statsd.Inc(fmt.Sprintf("app.log.messages,service=%s,level=DEBUG", service), 1, 0.1)
		} else {
			statsd.Inc(fmt.Sprintf("app.log.messages,service=%s,level=%s", service, level), 1, 1)
		}
	} else {
		statsd.Inc(fmt.Sprintf("app.log.messages,service=%s,level=OTHER", service), 1, 0.4)
	}

	return nil
}
