package producer

type Flags struct {
	Partitioner        string
	RequiredAcks       string
	MaxMessageBytes    int
	Partition          int32
	Separator          string
	LineSeparator      string
	File               string
	InputFormat        string
	Key                string
	Value              string
	NullValue          bool
	Headers            []string
	KeySchemaVersion   int
	ValueSchemaVersion int
	KeyEncoding        string
	ValueEncoding      string
	Silent             bool
	RateInSeconds      int
	ProtoFiles         []string
	ProtoImportPaths   []string
	ProtosetFiles      []string
	KeyProtoType       string
	ValueProtoType     string
}
