package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"github.com/Jeffail/gabs"
)

type Chunk struct {
	// Total number of chunks to be expected
	TotalCount int
	ReceivedCount int
	ReceivedBytes int
	Parts [][]byte

	Expiration int64

}

type Graylog struct {
	Port int

	Messages chan Message

	close chan bool

	Statsd StatisticsSender

	ReceivedChunks map[string]*Chunk

	LastCleanup int64
}

func (s *Graylog) RunCleanup() error {
	now := time.Now().UnixNano()
	for k, v := range s.ReceivedChunks {
		if now > v.Expiration {
			delete(s.ReceivedChunks, k)
		}
	}

	return nil
}

func (s *Graylog) HandleChunkedPacket(buffer []byte) error {

	message_id := string(buffer[2:10])

	c, found := s.ReceivedChunks[message_id]
	if !found {
		c = &Chunk{}
		// buffer[11] is Sequence count - 1 byte: Total number of chunks this message has.
		c.TotalCount = int(buffer[11])
		c.Parts = make([][]byte, c.TotalCount)

		// Mark expiration 5 seconds into the future
		c.Expiration = time.Now().UnixNano() + 5e9
	}

	// buffer[10] is Sequence number - 1 byte:
	// The sequence number of this chunk. Starting at 0 and always less than the sequence count.
	c.Parts[int(buffer[10])] = buffer[12:]

	c.ReceivedBytes += len(c.Parts[c.ReceivedCount])
	c.ReceivedCount += 1
	
	s.ReceivedChunks[message_id] = c

	if c.ReceivedCount == c.TotalCount {

		buf := make([]byte, c.ReceivedBytes)
		cursor := 0
		for _, sub := range c.Parts {
			l := len(sub)
			copy(buf[cursor:cursor+l], sub)
			cursor += l
		}

		m := Message{}
		m.Data = buf
		err := m.ParseJSON()
		if err != nil {
			return err
		}

		ConvertGraylogFields(&m)
		delete(s.ReceivedChunks, message_id)
		s.Messages <- m
	}

	if s.LastCleanup == 0 || time.Now().UnixNano() > s.LastCleanup + 5e9 {
		s.LastCleanup = time.Now().UnixNano()
		s.RunCleanup()
	}

	return nil
}


func (s *Graylog) ParseGraylogMessage(buffer []byte) (error) {
	m := Message{}

	if buffer[0] == '{' {
		// Non-chunked delivery
		m.Data = buffer

		err := m.ParseJSON()
		if err != nil {
			return err
		}

		ConvertGraylogFields(&m)

		s.Messages <- m
	} else if buffer[0] == 0x1E && buffer[1] == 0x0F {
		// Chunked delivery
		err := s.HandleChunkedPacket(buffer)
		if err != nil {
			return err
		}
	}

	return nil
}



func (s *Graylog) Init(port int) error {
	s.Port = port
	s.ReceivedChunks = make(map[string]*Chunk)
	s.close = make(chan bool)

	ServerAddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}

	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	if err != nil {
		return err
	}

	buf := make([]byte, 9500)

	go func() {
		for {
			select {
			case val, ok := <-s.close:
				if val == true || ok == false {
					return
				}
				break
			default:

				ServerConn.SetDeadline(time.Now().Add(time.Millisecond * 100))
				n, _, err := ServerConn.ReadFromUDP(buf)

				if s.Messages != nil && err == nil {

					err := s.ParseGraylogMessage(buf[0:n])
					if err != nil {
						if s.Statsd != nil {
							s.Statsd.Inc("logs2kafka.invalid_graylog_messages", 1, 0.1)
						}
						fmt.Fprintf(os.Stderr, "Error parsing json message: %s\n", err)
					}
				}
			}
		}
	}()

	return nil

}

func (s *Graylog) Close() {
	s.close <- true
}


/*

{
  \"version\": \"1.1\",
  \"host\": \"delivery-staging-us-east-1b-master-0\",
  \"short_message\": \"moi\r\",
  \"timestamp\": 1495112212.5,
  \"level\": 6,
  \"_app\": \"test\",
  \"_command\": \"bash\",
  \"_container_id\": \"1f7665c78d21073a0c5d58bc5ff6155ee2cce4f4069c234c57ed9bd7e048cb7f\",
  \"_container_name\": \"berserk_golick\",
  \"_created\": \"2017-05-18T12:56:45.918230564Z\",
  \"_image_id\": \"sha256:ebcd9d4fca80e9e8afc525d8a38e7c56825dfb4a220ed77156f9fb13b14d4ab7\",
  \"_image_name\": \"ubuntu:16.04\",
  \"_tag\": \"test\"
}

_container_id
_image_name

label:
_app
io.kubernetes.pod.name
io.kubernetes.container.name (use this as the topic name)
*/

func ConvertGraylogFields(m *Message) error {

	container_name, ok := m.Container.Path("_container_name").Data().(string)

	if ok {
		m.Container.Set(container_name, "container_name")
		m.Container.Delete("_container_name")
	}

	level_number, ok := m.Container.Path("level").Data().(float64)
	if ok {
		m.Container.Delete("level")
		switch level_number {
			case 3:
				m.Container.Set("ERROR", "level")
			case 4:
				m.Container.Set("WARN", "level")
			case 6:
				m.Container.Set("INFO", "level")
			case 7:
				m.Container.Set("DEBUG", "level")
			default:
				m.Container.Set("UNKNOWN", "level")
		}
	}

	// We just drop the float timestamp and generate our own ts field later
	m.Container.Delete("timestamp")

	short_message, ok := m.Container.Path("short_message").Data().(string)
	if ok {

		// Check if the short_message is in fact a JSON document and handle that
		if len(short_message) > 1 && short_message[0] == '{' {
			short_message_data := m.Container.Path("short_message").Data().(string)
			parsed_json_message, err := gabs.ParseJSON([]byte(short_message_data))

			if err != nil {
				// Error in JSON parse, just pass the data through as-is
				m.Container.Set(short_message, "msg")
			} else {

				children, _ := parsed_json_message.ChildrenMap()
				for key, value := range children {
					_, err = m.Container.Set(value.Data(), key)
				}
			}
		} else {
			m.Container.Set(short_message, "msg")
		}

		m.Container.Delete("short_message")
	}

	// Process tag. We can extract json properties out with the "key1=value1,key2=value2" notation
	tag, ok := m.Container.Path("_tag").Data().(string)
	if ok {
		parts := strings.Split(tag, ",")
		replaced := false
		for _, part := range parts {
			keyvalue := strings.Split(part, "=")
			if len(keyvalue) == 2 {
				m.Container.Set(keyvalue[1], keyvalue[0])
				replaced = true
			}
		}

		// Delete the _tag if we did parse things out from it
		if replaced {
			m.Container.Delete("_tag")
		}
	}

	// Convert registry2.applifier.info:5005/comet-source-adapter@sha256:f205ed11f1a26bb8ceefc9389ebe6
	// to registry2.applifier.info:5005/comet-source-adapter:f205ed11f1a26bb8ceefc9389ebe6
	// But only if there is no docker_image already set as this could be passed from tag handling
	_, ok = m.Container.Path("docker_image").Data().(string)
	if !ok {
		image_name, ok := m.Container.Path("_image_name").Data().(string)
		if ok {
			m.Container.Set(image_name, "docker_image")
		}
	}

	return nil
}