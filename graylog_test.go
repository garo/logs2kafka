package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"net"
	"time"
)

func TestGraylogMessageConversion(t *testing.T) {

	str := `{"version":"1.1",
	"host":"delivery-staging-us-east-1b-master-0",
	"short_message":"moi\r",
	"timestamp":1.495174443806e+09,
	"level":3,
	"_command":"/hyperkube controller-manager --master=http://127.0.0.1:8080 --service-account-private-key-file=/etc/kubernetes/ssl/client-server-key.pem --root-ca-file=/etc/kubernetes/ssl/ca.pem --leader-elect=true --cloud-provider=aws",
	"_container_id":"e02acf37f963ec2d46ede21766070559aa4a79c1afc8582e27d580ef2326800e",
	"_container_name":"k8s_kube-controller-manager_kube-controller-manager-ip-172-16-1-150.ec2.internal_kube-system_b50f4a11f4401d857fc4bbb355fceae8_0",
	"_created":"2017-05-19T06:14:02.196020749Z",
	"_image_id":"sha256:91a48ff795cebb88646ec6cc4955bf7f500466feb44ec18b508f2ce94ed0d9b7",
	"_image_name":"quay.io/coreos/hyperkube@sha256:77b81b118e6e231d284e6ae0ec50d898dadd88af469df33d5cf3f3a2d0d44473",
	"_io.kubernetes.container.name":"kube-controller-manager",
	"_io.kubernetes.pod.name":"kube-controller-manager-ip-172-16-1-150.ec2.internal",
	"_tag":"test"}`
	m := JSONToMessage(str)
	err := m.ParseJSON()
	assert.Nil(t, err)

	ConvertGraylogFields(&m)

	value, ok := m.Container.Path("msg").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "moi\r")

	value, ok = m.Container.Path("level").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "ERROR")

}

func TestGraylogMessageConversionForPodMessage(t *testing.T) {

	str := `
	{"version":"1.1",
	"host":"delivery-staging-us-east-1b-asg-general",
	"short_message":"main.main()",
	"timestamp":1.495173922171e+09,
	"level":7,
	"_command":"/comet-source-adapter","_container_id":"de57c44274f63c39455041c9515c0e12b856bef6533f8e4f8b3489852a0d7208",
	"_container_name":"k8s_ads-auction-comet-source-adapter_ads-auction-835331642-rzzgr_default_99baeb50-3bc3-11e7-a061-0a50dcb4a89e_1",
	"_created":"2017-05-19T06:05:22.037120689Z",
	"_image_id":"sha256:321a61fdf20848467b4eedc1f8d113308785eccefa21d2014b96d48021cbb309",
	"_image_name":"registry2.applifier.info:5005/comet-source-adapter@sha256:f205ed11f1a26bbfe3dd127adcf155949b9fb205b19821c8b78ceefc9389ebe6",
	"_io.kubernetes.container.name":"ads-auction-comet-source-adapter",
	"_io.kubernetes.pod.name":"ads-auction-835331642-rzzgr",
	"_tag":"test"
	}`

	m := JSONToMessage(str)
	err := m.ParseJSON()
	assert.Nil(t, err)

	ConvertGraylogFields(&m)

	value, ok := m.Container.Path("container_name").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "k8s_ads-auction-comet-source-adapter_ads-auction-835331642-rzzgr_default_99baeb50-3bc3-11e7-a061-0a50dcb4a89e_1")

	value, ok = m.Container.Path("level").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "DEBUG")

	value, ok = m.Container.Path("docker_image").Data().(string)
	assert.Equal(t, ok, true)
	assert.Equal(t, value, "registry2.applifier.info:5005/comet-source-adapter:f205ed11f1a26bbfe3dd127adcf155949b9fb205b19821c8b78ceefc9389ebe6")

}

func TestGraylogReceive(t *testing.T) {

	s := Graylog{}

	s.Messages = make(chan Message)

	err := s.Init(9998)
	assert.Nil(t, err)
	assert.Equal(t, 9998, s.Port)

	ServerAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:9998")
	assert.Nil(t, err)
	LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	assert.Nil(t, err)

	Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
	assert.Nil(t, err)

	defer Conn.Close()
	_, err = Conn.Write([]byte(`{"version":"1.1","host":"delivery-staging-us-east-1b-asg-general","short_message":"main.main()","timestamp":1.495173922171e+09,"level":3,"_command":"/comet-source-adapter","_container_id":"de57c44274f63c39455041c9515c0e12b856bef6533f8e4f8b3489852a0d7208","_container_name":"k8s_ads-auction-comet-source-adapter_ads-auction-835331642-rzzgr_default_99baeb50-3bc3-11e7-a061-0a50dcb4a89e_1","_created":"2017-05-19T06:05:22.037120689Z","_image_id":"sha256:321a61fdf20848467b4eedc1f8d113308785eccefa21d2014b96d48021cbb309","_image_name":"registry2.applifier.info:5005/comet-source-adapter@sha256:f205ed11f1a26bbfe3dd127adcf155949b9fb205b19821c8b78ceefc9389ebe6","_io.kubernetes.container.name":"ads-auction-comet-source-adapter","_io.kubernetes.pod.name":"ads-auction-835331642-rzzgr","_tag":"test"}`))
	assert.Nil(t, err)
	Conn.Close()

	msg := <-s.Messages
	value, ok := msg.Container.Path("msg").Data().(string)
	assert.Equal(t, true, ok)
	assert.Equal(t, "main.main()", value)

	s.Close()

}

func TestGraylogChunkProcessSingle(t *testing.T) {

	s := Graylog{}

	s.ReceivedChunks = make(map[string]*Chunk)

	s.HandleChunkedPacket([]byte("\x1e\x0f\x00\x00\x00\x00\xDE\xAD\xBE\xEF\x00\x02{\"version\":\"1.1\",\"host\":\"delivery-staging-us-east-1b-asg-general\",\"test\":\"foobar\""))

	chunk, found := s.ReceivedChunks["\x00\x00\x00\x00\xDE\xAD\xBE\xEF"]
	assert.Equal(t, true, found)
	assert.Equal(t, 1, chunk.ReceivedCount)
	assert.Equal(t, 2, chunk.TotalCount)
	assert.Equal(t, byte('{'), chunk.Parts[0][0])

}

func TestGraylogChunkProcessSingle2(t *testing.T) {

	s := Graylog{}

	s.ReceivedChunks = make(map[string]*Chunk)

	s.HandleChunkedPacket([]byte("\x1e\x0f\x00\x00\x00\x00\xDE\xAD\xBE\xEF\x01\x02,\"test2\":\"hello\"}"))

	chunk, found := s.ReceivedChunks["\x00\x00\x00\x00\xDE\xAD\xBE\xEF"]
	assert.Equal(t, true, found)
	assert.Equal(t, 1, chunk.ReceivedCount)
	assert.Equal(t, 2, chunk.TotalCount)
	assert.Equal(t, byte(','), chunk.Parts[1][0])
	assert.Equal(t, byte('"'), chunk.Parts[1][1])
}

func TestGraylogChunkProcessComplete(t *testing.T) {

	s := Graylog{}

	s.ReceivedChunks = make(map[string]*Chunk)
	s.Messages = make(chan Message, 10)

	s.HandleChunkedPacket([]byte("\x1e\x0f\x00\x00\x00\x00\xDE\xAD\xBE\xEF\x00\x02{\"version\":\"1.1\",\"host\":\"delivery-staging-us-east-1b-asg-general\",\"test\":\"foobar\",\"level\":7"))

	assert.Equal(t, 1, len(s.ReceivedChunks))
	
	s.HandleChunkedPacket([]byte("\x1e\x0f\x00\x00\x00\x00\xDE\xAD\xBE\xEF\x01\x02,\"test2\":\"hello\"}"))

	msg := <-s.Messages
	value, ok := msg.Container.Path("test").Data().(string)
	assert.Equal(t, true, ok)
	assert.Equal(t, "foobar", value)

	value, ok = msg.Container.Path("test2").Data().(string)
	assert.Equal(t, true, ok)
	assert.Equal(t, "hello", value)

	// Ensure that ConvertGraylogFields has been called
	value, ok = msg.Container.Path("level").Data().(string)
	assert.Equal(t, true, ok)
	assert.Equal(t, "DEBUG", value)

	assert.Equal(t, 0, len(s.ReceivedChunks))

}

func TestGraylogCleanupWorks(t *testing.T) {

	s := Graylog{}

	s.ReceivedChunks = make(map[string]*Chunk)
	s.Messages = make(chan Message, 10)

	s.HandleChunkedPacket([]byte("\x1e\x0f\x00\x00\x00\x00\xDE\xAD\xBE\xEF\x00\x02{\"version\":\"1.1\",\"host\":\"delivery-staging-us-east-1b-asg-general\",\"test\":\"foobar\""))

	assert.Equal(t, 1, len(s.ReceivedChunks))
	s.RunCleanup()
	assert.Equal(t, 1, len(s.ReceivedChunks))

	chunk := s.ReceivedChunks["\x00\x00\x00\x00\xDE\xAD\xBE\xEF"]
	chunk.Expiration -= 6e9 // 6 seconds
	s.RunCleanup()
	assert.Equal(t, 0, len(s.ReceivedChunks))

}


func TestGraylogRunCleanupEvery5Seconds(t *testing.T) {

	s := Graylog{}

	s.ReceivedChunks = make(map[string]*Chunk)
	s.Messages = make(chan Message, 10)
	s.LastCleanup = time.Now().UnixNano()

	s.HandleChunkedPacket([]byte("\x1e\x0f\x00\x00\x00\x00\xDE\xAD\xBE\xEF\x00\x02{\"version\":\"1.1\",\"host\":\"delivery-staging-us-east-1b-asg-general\",\"test\":\"foobar\""))

	// Simulate that six seconds have passed
	chunk := s.ReceivedChunks["\x00\x00\x00\x00\xDE\xAD\xBE\xEF"]
	chunk.Expiration -= 6e9

	// A call to HandleChunkedPacket should trigger RunCleanup if previous
	// was done over 5 seconds ago.
	//
	// But since LastCleanup is not changed sending another packet 
	// not trigger any cleanup
	s.HandleChunkedPacket([]byte("\x1e\x0f\x00\x00\x00\x00\xDE\xAD\x00\x00\x00\x02{\"version\":\"1.1\",\"host\":\"delivery-staging-us-east-1b-asg-general\",\"test\":\"foobar\""))
	assert.Equal(t, 2, len(s.ReceivedChunks))

	// After this the next HandleChunkedPacket should trigger cleanup
	s.LastCleanup -= 6e9

	s.HandleChunkedPacket([]byte("\x1e\x0f\x00\x00\x00\x00\x00\xAD\xBE\xEF\x00\x02{\"version\":\"1.1\",\"host\":\"delivery-staging-us-east-1b-asg-general\",\"test\":\"foobar\""))

	assert.Equal(t, 2, len(s.ReceivedChunks))

}

/*
func TestGraylogFullChunkReceive(t *testing.T) {

	s := Graylog{}

	s.Messages = make(chan Message)

	err := s.Init(9997)
	assert.Nil(t, err)
	assert.Equal(t, 9997, s.Port)

	ServerAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:9997")
	assert.Nil(t, err)
	LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	assert.Nil(t, err)

	Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
	assert.Nil(t, err)

	defer Conn.Close()
	_, err = Conn.Write([]byte("\x1e\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02{\"version\":\"1.1\",\"host\":\"delivery-staging-us-east-1b-asg-general\",\"test\":\"foobar\""))
	
	_, err = Conn.Write([]byte("\x1e\x0f\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02,\"short_message\":\"main.main()\",\"timestamp\":1.495173922171e+09,\"level\":3,\"_command\":\"/comet-source-adapter\",\"_container_id\":\"de57c44274f63c39455041c9515c0e12b856bef6533f8e4f8b3489852a0d7208\",\"_container_name\":\"k8s_ads-auction-comet-source-adapter_ads-auction-835331642-rzzgr_default_99baeb50-3bc3-11e7-a061-0a50dcb4a89e_1\",\"_created\":\"2017-05-19T06:05:22.037120689Z\",\"_image_id\":\"sha256:321a61fdf20848467b4eedc1f8d113308785eccefa21d2014b96d48021cbb309\",\"_image_name\":\"registry2.applifier.info:5005/comet-source-adapter@sha256:f205ed11f1a26bbfe3dd127adcf155949b9fb205b19821c8b78ceefc9389ebe6\",\"_io.kubernetes.container.name\":\"ads-auction-comet-source-adapter\",\"_io.kubernetes.pod.name\":\"ads-auction-835331642-rzzgr\",\"_tag\":\"test\"}"))
	assert.Nil(t, err)
	Conn.Close()

	msg := <-s.Messages
	value, ok := msg.Container.Path("msg").Data().(string)
	assert.Equal(t, true, ok)
	assert.Equal(t, "main.main()", value)

	value, ok = msg.Container.Path("version").Data().(string)
	assert.Equal(t, true, ok)
	assert.Equal(t, "1.1", value)

	value, ok = msg.Container.Path("_command").Data().(string)
	assert.Equal(t, true, ok)
	assert.Equal(t, "/comet-source-adapter", value)


	s.Close()

}
*/