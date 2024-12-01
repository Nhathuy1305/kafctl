package topic

import (
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	"kafctl/internal"
	"os"
	"strings"
)

type Topic struct {
	Name       string
	Partitions []Partition       `json:",omitempty" yaml:",omitempty"`
	Configs    []internal.Config `json:",omitempty" yaml:",omitempty"`
}

type Partition struct {
	ID           int32
	OldestOffset int64   `json:"oldestOffset" yaml:"oldestOffset"`
	NewestOffset int64   `json:"newestOffset" yaml:"newestOffset"`
	Leader       string  `json:",omitempty" yaml:",omitempty"`
	Replicas     []int32 `json:",omitempty" yaml:",omitempty,flow"`
	ISRs         []int32 `json:"inSyncReplicas,omitempty" yaml:"inSyncReplicas,omitempty,flow"`
}

type requestedTopicFields struct {
	partitionID       bool
	partitionOffset   bool
	partitionLeader   bool
	partitionReplicas bool
	partitionISRs     bool
	config            PrintConfigsParam
}

var allFields = requestedTopicFields{
	partitionID: true, partitionOffset: true, partitionLeader: true,
	partitionReplicas: true, partitionISRs: true, config: NonDefaultConfigs,
}

type GetTopicsFlags struct {
	OutputFormat string
}

type CreateTopicFlags struct {
	Partitions        int32
	ReplicationFactor int16
	ValidateOnly      bool
	File              string
	Configs           []string
}

type AlterTopicFlags struct {
	Partitions        int32
	ReplicationFactor int16
	ValidateOnly      bool
	Configs           []string
}

type DeleteRecordsFlags struct {
	Offsets []string
}

type PrintConfigsParam string

const (
	NoConfigs         PrintConfigsParam = "none"
	AllConfigs        PrintConfigsParam = "all"
	NonDefaultConfigs PrintConfigsParam = "no_defaults"
)

type DescribeTopicFlags struct {
	PrintConfigs        PrintConfigsParam
	SkipEmptyPartitions bool
	OutputFormat        string
}

type Operation struct {
}

func (operation *Operation) CreateTopics(topics []string, flags CreateTopicFlags) error {
	var (
		err     error
		context internal.ClientContext
		admin   sarama.ClusterAdmin
	)

	if context, err = internal.CreateClientContext(); err != nil {
		return err
	}

	if admin, err = internal.CreateClusterAdmin(&context); err != nil {
		return errors.Wrap(err, "failed to create cluster admin")
	}

	topicDetails := sarama.TopicDetail{
		NumPartitions:     flags.Partitions,
		ReplicationFactor: flags.ReplicationFactor,
		ConfigEntries:     map[string]*string{},
	}

	for _, config := range flags.Configs {
		configParts := strings.Split(config, "=")
		topicDetails.ConfigEntries[configParts[0]] = &configParts[1]
	}

	if flags.File != "" {
		fileContent, err := os.ReadFile(flags.File)
		if err != nil {
			return errors.Wrap(err, "could not read topic description file")
		}

		fileTopicConfig := Topic{}
	}
}
