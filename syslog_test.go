package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"net"
)

func TestExtractPriority(t *testing.T) {
	buff := []byte("<27>")
	start := 0

	pri, err := ExtractPriority(buff, &start, len(buff))
	assert.Nil(t, err)
	assert.Equal(t, pri.Priority, 27)
	assert.Equal(t, pri.Facility, 3)
	assert.Equal(t, pri.Severity, 3)

}

func TestParseSyslogMessage(t *testing.T) {

	m, err := ParseSyslogMessage([]byte("<27>Aug  7 18:33:19 HOSTNAME docker/container-name/id/registry:5000/foobar:12341234[9103]: Hello from Docker."))
	assert.Nil(t, err)

	value, ok := m.Container.Path("container_name").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "container-name")

	value, ok = m.Container.Path("container_id").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "id")

	value, ok = m.Container.Path("docker_image").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "registry:5000/foobar:12341234")

}

func TestParseSyslogMessage2(t *testing.T) {

	m, err := ParseSyslogMessage([]byte("<27>2016-06-09T04:47:45Z as-ads-brand-i-34d06ca8 docker/ads-publisher-sync/e3088a0601ea/registry.applifier.info:5000/ads-publisher-sync:c48b56ff43e1cce471a19015515a3e4706277d06[1084]: 2016/06/09 04:47:45 Setting up publisher sync service with debugging: true"))
	assert.Nil(t, err)

	value, ok := m.Container.Path("container_name").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "ads-publisher-sync")

	value, ok = m.Container.Path("container_id").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "e3088a0601ea")

	value, ok = m.Container.Path("docker_image").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "registry.applifier.info:5000/ads-publisher-sync:c48b56ff43e1cce471a19015515a3e4706277d06")

}


func TestParseSyslogMessageJSON(t *testing.T) {

	m, err := ParseSyslogMessage([]byte("<27>Aug  7 18:33:19 HOSTNAME docker/container-name/id/registry:5000/foobar:123412341234[9103]: {\"service\":\"foobar\",\"level\":\"DEBUG\",\"msg\":\"Hello, World!\\n\"}"))
	assert.Nil(t, err)

	value, ok := m.Container.Path("msg").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "Hello, World!\n")

	value, ok = m.Container.Path("level").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "DEBUG")

	value, ok = m.Container.Path("service").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "foobar")
}

func TestParseSyslogMessageWitthISOTimestamp(t *testing.T) {

	m, err := ParseSyslogMessage([]byte("<27>2016-06-06T13:24:36Z hvm-ami-builder docker/dreamy_ramanujan/0fc5ec54111c/ubuntu:14.04[27481]: Hello, World!"))
	assert.Nil(t, err)

	value, ok := m.Container.Path("msg").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "Hello, World!")

}

func TestParseSyslogMessageJSONWitthISOTimestamp(t *testing.T) {

	m, err := ParseSyslogMessage([]byte("<27>2016-06-06T13:24:36Z hvm-ami-builder docker/dreamy_ramanujan/0fc5ec54111c/ubuntu:14.04[27481]: {\"service\":\"foobar\",\"level\":\"DEBUG\",\"msg\":\"Hello, World!\\n\"}"))
	assert.Nil(t, err)

	value, ok := m.Container.Path("msg").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "Hello, World!\n")

	value, ok = m.Container.Path("level").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "DEBUG")

	value, ok = m.Container.Path("service").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "foobar")


}

func TestParseSyslogMessageJSONWitthISOTimestampAndMessageTimestamp(t *testing.T) {

	m, err := ParseSyslogMessage([]byte("<27>2016-06-06T13:24:36Z hvm-ami-builder docker/dreamy_ramanujan/0fc5ec54111c/ubuntu:14.04[27481]: 2016-06-08T12:13:14.123 {\"service\":\"foobar\",\"level\":\"DEBUG\",\"msg\":\"Hello, World!\\n\"}"))
	assert.Nil(t, err)

	value, ok := m.Container.Path("msg").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "Hello, World!\n")

	value, ok = m.Container.Path("level").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "DEBUG")

	value, ok = m.Container.Path("service").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "foobar")

}

func TestSyslog(t *testing.T) {

	s := Syslog{}

	s.Messages = make(chan Message)

	err := s.Init(9999)
	assert.Nil(t, err)
	assert.Equal(t, 9999, s.Port)

    ServerAddr, err := net.ResolveUDPAddr("udp","127.0.0.1:9999")
	assert.Nil(t, err)
    LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	assert.Nil(t, err)

    Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
	assert.Nil(t, err)
 
    defer Conn.Close()
    _,err = Conn.Write([]byte("<27>Aug  7 18:33:19 HOSTNAME docker/hello-world/foobar/5790672ab6a0[9103]: Hello from Docker."))
    assert.Nil(t, err)
    Conn.Close()


    msg := <-s.Messages
    value, ok := msg.Container.Path("msg").Data().(string)
    assert.Equal(t, true, ok)
    assert.Equal(t, "Hello from Docker.", value)

    s.Close()

}
