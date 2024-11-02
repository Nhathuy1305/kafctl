package internal

import (
	"github.com/IBM/sarama"
	"regexp"
	"time"
)

var (
	invalidClientIDCharactersRegExp = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
)

type TokenProvider struct {
	PluginName string
	Options    map[string]any
}

type SaslConfig struct {
	Enabled       bool
	Username      string
	Password      string
	Mechanism     string
	TokenProvider TokenProvider
}

type AvroConfig struct {
	SchemaRegistry string
	JSONCodec      avro.JSONCodec
	RequestTimeout time.Duration
	TLS            TLSConfig
	Username       string
	Password       string
}

type TLSConfig struct {
	Enabled  bool
	CA       string
	Cert     string
	CertKey  string
	Insecure bool
}

type K8sToleration struct {
	Key      string `json:"key" yaml:"key"`
	Operator string `json:"operator" yaml:"operator"`
	Value    string `json:"value" yaml:"value"`
	Effect   string `json:"effect" yaml:"effect"`
}

type K8sConfig struct {
	Enabled         bool
	Binary          string
	KubeConfig      string
	KubeContext     string
	Namespace       string
	Image           string
	ImagePullSecret string
	ServiceAccount  string
	KeepPod         bool
	Labels          map[string]string
	Annotations     map[string]string
	NodeSelector    map[string]string
	Affinity        map[string]any
	Tolerations     []K8sToleration
}

type ConsumerConfig struct {
	IsolationLevel string
}

type ProducerConfig struct {
	Partitioner    string
	RequiredAcks   string
	MaxMessageByes int
}

type ClientContext struct {
	Name           string
	Brokers        []string
	TLS            TLSConfig
	Sasl           SaslConfig
	Kubernetes     K8sConfig
	RequestTimeout time.Duration
	ClientID       string
	KafkaVersion   sarama.KafkaVersion
	Avro           AvroConfig
	Protobuf       protobuf.SearchContext
	Producer       ProducerConfig
	Consumer       ConsumerConfig
}

type Config struct {
	Name  string
	Value string
}
