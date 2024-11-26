package producer

import "github.com/IBM/sarama"

type MessageSerializer interface {
	CanSerialize(topic string) (bool, error)
	Serialize(key, value []byte, flags Flags) (*sarama.ProducerMessage, error)
}

const (
	HEX    = "hex"
	BASE64 = "base64"
)

func createRecordHeaders(flags Flags) ([]sarama.RecordHeader, error) {
	recordHeaders := make([]sarama.RecordHeader, len(flags.Headers))

	for i, header := range flags.Headers {
		key, value, err :=
	}
}
