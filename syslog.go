package main

import "net"
import "strconv"
import "strings"
import "github.com/Jeffail/gabs"
import "time"
import "errors"
import "fmt"
import "os"

type StringWriter interface {
	WriteString(string) (int, error)
}

type Syslog struct {
	Port int

	Messages chan Message

	close chan bool

	Statsd StatisticsSender
}

func (s *Syslog) Init(port int) error {
	s.Port = port
	s.close = make(chan bool)

	ServerAddr,err := net.ResolveUDPAddr("udp",":" + strconv.Itoa(port))
	if err != nil {
		return err
	}

	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	if err != nil {
		return err
	}


	buf := make([]byte, 9000)

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
				n,_,err := ServerConn.ReadFromUDP(buf)

				if s.Messages != nil && err == nil {

					msg, err := ParseSyslogMessage(buf[0:n])
					if err == nil {
						s.Messages <- msg
					} else {
						if s.Statsd != nil {
							s.Statsd.Inc("logs2kafka.invalid_messages", 1, 0.1)
						}
						fmt.Fprintf(os.Stderr, "Error parsing json message: %s\n", err)
					}
				}
			}
		}
	}()

	return nil
	
}

func (s *Syslog) Close() {
	s.close <- true
}

type Priority struct {
	Priority int
	Facility int
	Severity int
}

func IsDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func FindNextSpace(buff []byte, from int, l int) (int, error) {
	var to int

	for to = from; to < l; to++ {
		if buff[to] == ' ' {
			to++
			return to, nil
		}
	}

	return 0, errors.New("No space found")
}

func ExtractPriority(buffer []byte, cursor *int, l int) (Priority, error) {

	priority := Priority{}

	if buffer[*cursor] != '<' {
		return priority, errors.New("No priority start character")
	}

	i := 1 // Start after '<'
	priDigit := 0

	for i < l {
		if i >= 5 {
			return priority, errors.New("No priority end character or priority too long")
		}

		c := buffer[i]

		if c == '>' {
			if i == 1 {
				return priority, errors.New("Priority too short")
			}

			*cursor = i + 1
			priority.Priority = priDigit
			priority.Facility = priDigit / 8
			priority.Severity = priDigit % 8
			return priority, nil
		}

		if IsDigit(c) {
			v, e := strconv.Atoi(string(c))
			if e != nil {
				return priority, e
			}

			priDigit = (priDigit * 10) + v
		} else {
			return priority, errors.New("Priority was not valid digit")
		}

		i++
	}

	return priority, errors.New("No end found")	
}

func ParseSyslogMessage(buffer []byte) (Message, error) {
	m := Message{}
	var err error

	var parts []string
	var tags []string
	var payload string

	cursor := 0
	l := len(buffer)

	_, err = ExtractPriority(buffer, &cursor, l)
	if err != nil {
		return m, err
	}

	stringbuffer := string(buffer[cursor:])

	parts = strings.SplitN(stringbuffer, " ", 7)
	//for i := 0; i < len(parts); i++ {
	//	fmt.Printf("parts[%d]: %s\n", i, parts[i])
	//}

	// Find out if date format is ISO8601
	if len(parts) > 0 && len(parts[0]) > 18 && parts[0][10] == 'T' {

		tags = strings.SplitN(parts[2], "/", 4)
		//fmt.Printf("ISO8601 tags: %+v, len: %d\n", tags, len(tags))
		if len(tags) != 4 {
			return m, errors.New("Malformed input on phase 2, assuming ISO8601 date format")
		}
		payload = strings.SplitN(stringbuffer, " ", 4)[3]

	} else {
		if len(parts) < 6 {
			return m, errors.New("Malformed input on phase 1, assuming legacy date format")
		}

		tags = strings.SplitN(parts[5], "/", 4)
		//fmt.Printf("tags: %+v, len: %d", tags, len(tags))
		if len(tags) != 4 {
			return m, errors.New("Malformed input on phase 2, assuming legacy date format")
		}

		payload = parts[6]
	}

	//fmt.Printf("Payload after detection: %+v\n", payload)

	// Simple JSON detection
	if payload[0] == '{' {
		m = JSONToMessage(payload)
		err := m.ParseJSON()
		if err != nil {
			m.Container = gabs.New()
			m.Container.Set(payload, "msg")
		}

	} else if len(payload) > 25 && payload[10] == 'T' && payload[24] == '{' { // Detect payload which has ISO8601 timestamp in the beginning
		m = JSONToMessage(payload[24:])
		err = m.ParseJSON()		
		if err != nil {
			m.Container = gabs.New()
			m.Container.Set(payload, "msg")
		}
	} else {
		m.Container = gabs.New()
		m.Container.Set(payload, "msg")
	}

	m.Container.Set(tags[1], "container_name")
	m.Container.Set(tags[2], "container_id")

	n := strings.Index(tags[3], "[")
	if n != -1 {
		m.Container.Set(tags[3][0:n], "docker_image")
	} else {
		m.Container.Set(tags[3], "docker_image")
	}

	return m, nil
}

