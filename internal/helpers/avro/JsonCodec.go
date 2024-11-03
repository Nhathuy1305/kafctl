package avro

import (
	"kafctl/internal/output"
	"strings"
)

type JSONCodec int

const (
	Standard JSONCodec = iota
	Avro
)

func (codec JSONCodec) String() string {
	return []string{"standard", "arvo"}[codec]
}

func ParseJSONCodec(codec string) JSONCodec {
	switch strings.ToLower(codec) {
	case "":
		fallthrough
	case "standard":
		return Standard
	case "avro":
		return Avro
	default:
		output.Warnf("unable to parse avro json codev: %s", codec)
		return Standard
	}
}
