package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"fmt"
	"os"
	"strings"
)

func TestKafkaProducer(t *testing.T) {

	k := KafkaProducer{}

	var brokers_str string
	brokers_str, found := os.LookupEnv("KAFKA_CONNECTION_STRING")
	if !found {
		return
	}

	brokers := strings.Split(brokers_str, ",")


	err := k.Init(brokers, "mykey")
	assert.Nil(t, err)


	for i := 0; i < 16; i++ {
		m := JSONToMessage(fmt.Sprintf("{\"level\":\"DEBUG\",\"msg\":\"Hello, World: %d!\\n\"}", i))
		err := m.ParseJSON()
		assert.Nil(t, err)

		k.Produce(m)		
	}

	k.Close()

}
