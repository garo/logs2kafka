package main

import "gopkg.in/Shopify/sarama.v1"
import "time"
import "fmt"
import "hash"
import "hash/fnv"

type KafkaProducer struct {
	Brokers []string

	CommonKey sarama.Encoder

	producer sarama.AsyncProducer

	Statsd StatisticsSender
}

type inconsistentHashPartitioner struct {
	random sarama.Partitioner
	hasher hash.Hash32
}

func NewInconsistentHashPartitioner(topic string) sarama.Partitioner {
	p := new(inconsistentHashPartitioner)
	p.random = sarama.NewRandomPartitioner("")
	p.hasher = fnv.New32a()
	return p
}

func (p *inconsistentHashPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return p.random.Partition(message, numPartitions)
	}
	bytes, err := message.Key.Encode()
	if err != nil {
		return -1, err
	}
	p.hasher.Reset()
	_, err = p.hasher.Write(bytes)
	if err != nil {
		return -1, err
	}
	hash := int32(p.hasher.Sum32())
	if hash < 0 {
		hash = -hash
	}
	return hash % numPartitions, nil
}

func (p *inconsistentHashPartitioner) RequiresConsistency() bool {
	return false
}

func (s *KafkaProducer) Init(brokers []string, partition_key string) error {
	s.Brokers = brokers

	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = false
	conf.Producer.Return.Errors = true
	conf.Producer.Partitioner = NewInconsistentHashPartitioner
	conf.Producer.Flush.Messages = 1
	conf.Producer.Flush.Frequency = 20 * time.Millisecond
	//conf.Producer.RequiredAcks = -1
	conf.Metadata.Retry.Max = 1

	s.CommonKey = sarama.ByteEncoder(partition_key)

	kp, err := sarama.NewAsyncProducer(brokers, conf)

	if err != nil {
		return err
	}

	s.producer = kp

	go func() {
		for v := range kp.Errors() {
			fmt.Printf("errors: %+v\n", v.Msg)
			fmt.Printf("v: %+v\n", v)
			if s.Statsd != nil {
				s.Statsd.Inc("logs2kafka.produceErrors", 1, 1)
			}
		}
	}()
	return nil

}

func (s *KafkaProducer) Produce(m Message) {

	km := sarama.ProducerMessage{
		Topic: m.Topic,
		Key:   s.CommonKey,
		Value: sarama.ByteEncoder(m.Container.Bytes()),
	}
	//fmt.Printf("producer: %+v\n", s.producer)
	s.producer.Input() <- &km
}

func (s *KafkaProducer) Close() {
	//s.close <- true
	s.producer.AsyncClose()
}
