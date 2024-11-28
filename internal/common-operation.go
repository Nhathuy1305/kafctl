package internal

import (
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	"github.com/riferrei/srclient"
	"github.com/spf13/viper"
	"kafctl/internal/auth"
	"kafctl/internal/global"
	"kafctl/internal/helpers"
	"kafctl/internal/helpers/avro"
	"kafctl/internal/helpers/protobuf"
	"kafctl/internal/output"
	"os/user"
	"regexp"
	"strings"
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
	Partitioner     string
	RequiredAcks    string
	MaxMessageBytes int
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

func CreateClientContext() (ClientContext, error) {
	var context ClientContext

	context.Name = global.GetCurrentContext()

	if viper.Get("context."+context.Name) == nil {
		return context, errors.Errorf("no context with name %s found", context.Name)
	}

	if viper.GetString("contexts."+context.Name+".tlsCA") != "" ||
		viper.GetString("contexts."+context.Name+".tlsCert") != "" ||
		viper.GetString("contexts."+context.Name+".tlsCertKey") != "" ||
		viper.GetString("contexts."+context.Name+".tlsInsecure") != "" {
		return context, errors.Errorf("Your tls config contains fields that are not longer supported. Please update your config")
	}

	context.Brokers = viper.GetStringSlice("contexts." + context.Name + ".brokers")
	context.TLS.Enabled = viper.GetBool("contexts." + context.Name + ".tls.enabled")
	context.TLS.CA = viper.GetString("contexts." + context.Name + ".tls.ca")
	context.TLS.Cert = viper.GetString("contexts." + context.Name + ".tls.cert")
	context.TLS.CertKey = viper.GetString("contexts." + context.Name + ".tls.certKey")
	context.TLS.Insecure = viper.GetBool("contexts." + context.Name + ".tls.insecure")
	context.ClientID = viper.GetString("contexts." + context.Name + ".clientID")

	context.RequestTimeout = viper.GetDuration("contexts." + context.Name + ".requestTimeout")

	if version, err := kafkaVersion(viper.GetString("contexts." + context.Name + ".kafkaVersion")); err == nil {
		context.KafkaVersion = version
	} else {
		return context, err
	}
	context.Avro.SchemaRegistry = viper.GetString("contexts" + context.Name + ".avro.schemaRegistry")
	context.Avro.JSONCodec = avro.ParseJSONCodec(viper.GetString("contexts" + context.Name + ".avro.jsonCodec"))
	context.Avro.RequestTimeout = viper.GetDuration("contexts." + context.Name + ".avro.requestTimeout")
	context.Avro.TLS.Enabled = viper.GetBool("contexts." + context.Name + ".avro.tls.enabled")
	context.Avro.TLS.CA = viper.GetString("contexts." + context.Name + ".avro.tls.ca")
	context.Avro.TLS.Cert = viper.GetString("contexts." + context.Name + ".avro.tls.cert")
	context.Avro.TLS.CertKey = viper.GetString("contexts." + context.Name + ".avro.tls.certKey")
	context.Avro.TLS.Insecure = viper.GetBool("contexts." + context.Name + ".avro.tls.insecure")
	context.Avro.Username = viper.GetString("contexts." + context.Name + ".avro.username")
	context.Avro.Password = viper.GetString("contexts." + context.Name + ".avro.password")
	context.Protobuf.ProtosetFiles = viper.GetStringSlice("contexts." + context.Name + ".protobuf.protosetFiles")
	context.Protobuf.ProtoImportPaths = viper.GetStringSlice("contexts." + context.Name + ".protobuf.importPaths")
	context.Protobuf.ProtoFiles = viper.GetStringSlice("contexts." + context.Name + ".protobuf.protoFiles")
	context.Producer.Partitioner = viper.GetString("contexts." + context.Name + ".producer.partitioner")
	context.Producer.RequiredAcks = viper.GetString("contexts." + context.Name + ".producer.requiredAcks")
	context.Producer.MaxMessageBytes = viper.GetInt("contexts." + context.Name + ".producer.maxMessageBytes")
	context.Consumer.IsolationLevel = viper.GetString("contexts." + context.Name + ".consumer.isolationLevel")
	context.Sasl.Enabled = viper.GetBool("contexts." + context.Name + ".sasl.enabled")
	context.Sasl.Username = viper.GetString("contexts." + context.Name + ".sasl.username")
	context.Sasl.Password = viper.GetString("contexts." + context.Name + ".sasl.password")
	context.Sasl.Mechanism = viper.GetString("contexts." + context.Name + ".sasl.mechanism")
	context.Sasl.TokenProvider.PluginName = viper.GetString("contexts." + context.Name + ".sasl.tokenProvider.plugin")
	context.Sasl.TokenProvider.Options = viper.GetStringMap("contexts." + context.Name + ".sasl.tokenProvider.options")

	viper.SetDefault("contexts."+context.Name+".kubernetes.binary", "kubectl")
	context.Kubernetes.Enabled = viper.GetBool("contexts." + context.Name + ".kubernetes.enabled")
	context.Kubernetes.Binary = viper.GetString("contexts." + context.Name + ".kubernetes.binary")
	context.Kubernetes.KubeConfig = viper.GetString("contexts." + context.Name + ".kubernetes.kubeConfig")
	context.Kubernetes.KubeContext = viper.GetString("contexts." + context.Name + ".kubernetes.kubeContext")
	context.Kubernetes.Namespace = viper.GetString("contexts." + context.Name + ".kubernetes.namespace")
	context.Kubernetes.Image = viper.GetString("contexts." + context.Name + ".kubernetes.image")
	context.Kubernetes.ImagePullSecret = viper.GetString("contexts." + context.Name + ".kubernetes.imagePullSecret")
	context.Kubernetes.ServiceAccount = viper.GetString("contexts." + context.Name + ".kubernetes.serviceAccount")
	context.Kubernetes.KeepPod = viper.GetBool("contexts." + context.Name + ".kubernetes.keepPod")
	context.Kubernetes.Labels = viper.GetStringMapString("contexts." + context.Name + ".kubernetes.labels")
	context.Kubernetes.Annotations = viper.GetStringMapString("contexts." + context.Name + ".kubernetes.annotations")
	context.Kubernetes.NodeSelector = viper.GetStringMapString("contexts." + context.Name + ".kubernetes.nodeSelector")
	context.Kubernetes.Affinity = viper.GetStringMap("contexts." + context.Name + ".kubernetes.affinity")

	if err := viper.UnmarshalKey("contexts."+context.Name+".kubernetes.tolerations", &context.Kubernetes.Tolerations); err != nil {
		return context, err
	}

	return context, nil
}

func CreateClient(context *ClientContext) (sarama.Client, error) {
	config, err := CreateClientConfig(context)
	if err == nil {
		return sarama.NewClient(context.Brokers, config)
	}
	return nil, err
}

func CreateClusterAdmin(context *ClientContext) (sarama.ClusterAdmin, error) {
	config, err := CreateClientConfig(context)
	if err == nil {
		return sarama.NewClusterAdmin(context.Brokers, config)
	}
	return nil, err
}

func CreateClientConfig(context *ClientContext) (*sarama.Config, error) {
	var config = sarama.NewConfig()
	config.Version = context.KafkaVersion
	config.ClientID = GetClientID(context, "kafctl-")

	if context.RequestTimeout > 0 {
		output.Debugf("using admin request timeout: %s", context.RequestTimeout.String())
		config.Admin.Timeout = context.RequestTimeout
	} else {
		output.Debugf("using default admin request timeout: 3s")
	}

	if context.TLS.Enabled {
		output.Debugf("SASL is enabled (usename = %s)", context.Sasl.Username)
	} else {
		output.Debugf("SASL is enabled")
	}
	config.Net.SASL.Enable = true
	config.Net.SASL.User = context.Sasl.Username
	config.Net.SASL.Password = context.Sasl.Password
	switch context.Sasl.Mechanism {
	case "scram-sha512":
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &helpers.XDGSCRAMClient{HashGeneratorFcn: helpers.SHA512}
		}
	case "scram-sha256":
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &helpers.XDGSCRAMClient{HashGeneratorFcn: helpers.SHA256}
		}
	case "oauth":
		config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	case "plaintext":
		fallthrough
	case "":
		break
	default:
		return nil, errors.Errorf("Unknown sasl mechanism: %s", context.Sasl.Mechanism)
	}

	if config.Net.SASL.Mechanism == sarama.SASLTypeOAuth {
		tokenProvider, err := auth.LoadTokenProviderPlugin(context.Sasl.TokenProvider.PluginName, context.Sasl.TokenProvider.Options, context.Brokers)
		if err != nil {
			return nil, errors.Wrap(err, "failed to load tokenProvider")
		}
		config.Net.SASL.TokenProvider = tokenProvider
	}

	return config, nil
}

func CreateAvroSchemaRegistryClient(context *ClientContext) (srclient.ISchemaRegistryClient, error) {

}

func GetClientID(context *ClientContext, defaultPrefix string) string {
	var (
		err error
		usr *user.User
	)

	if context.ClientID != "" {
		return context.ClientID
	} else if usr, err = user.Current(); err != nil {
		output.Warnf("Failed to read current user: %v", err)
		return strings.TrimSuffix(defaultPrefix, "-")
	}
	return defaultPrefix + sanitizeUsername(usr.Username)
}

func sanitizeUsername(u string) string {

}

func kafkaVersion(s string) (sarama.KafkaVersion, error) {

}
