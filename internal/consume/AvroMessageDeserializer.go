package consume

import "kafctl/internal/helpers/avro"

type AvroMessageDeserializer struct {
	topic     string
	jsonCodec avro.JSONCodec
	registry  *CachingSchemaRegistry
}
