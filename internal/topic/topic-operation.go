package topic

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"kafctl/internal"
	"kafctl/internal/output"
	"os"
	"path"
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
		ext := path.Ext(flags.File)
		var unmarshalErr error
		switch ext {
		case ".yml", ".yaml":
			unmarshalErr = yaml.Unmarshal(fileContent, &fileTopicConfig)
		case ".json":
			unmarshalErr = json.Unmarshal(fileContent, &fileTopicConfig)
		default:
			return errors.Wrapf(err, "unsupported file format '%s'", ext)
		}
		if unmarshalErr != nil {
			return errors.Wrap(err, "could not unmarshal config file")
		}

		numPartitions := int32(len(fileTopicConfig.Partitions))
		if flags.Partitions == 1 {
			topicDetails.NumPartitions = numPartitions
		}

		replicationFactors := map[int16]struct{}{}
		for _, partition := range fileTopicConfig.Partitions {
			replicationFactors[int16(len(partition.Replicas))] = struct{}{}
		}
		if flags.ReplicationFactor == -1 && len(replicationFactors) == 1 {
			topicDetails.ReplicationFactor = int16(len(fileTopicConfig.Partitions[0].Replicas))
		} else if flags.ReplicationFactor == -1 && len(replicationFactors) != 1 {
			output.Warnf("replication factor from file ignored. partitions have different replicaCounts.")
		}

		for _, v := range fileTopicConfig.Configs {
			if _, ok := topicDetails.ConfigEntries[v.Name]; !ok {
				topicDetails.ConfigEntries[v.Name] = &v.Value
			}
		}
	}

	for _, topic := range topics {
		if err = admin.CreateTopic(topic, &topicDetails, flags.ValidateOnly); err != nil {
			return errors.Wrap(err, "failed to create topic")
		}
		output.Infof("topic created: %s", topic)
	}
	return nil
}

func (operation *Operation) DeleteTopics(topics []string) error {
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

	for _, topic := range topics {
		if err = admin.DeleteTopic(topic); err != nil {
			return errors.Wrap(err, "failed to delete topic")
		}
		output.Infof("topic deleted: %s", topic)
	}
	return nil
}

func (operation *Operation) DescribeTopic(topic string, flags DescribeTopicFlags) error {
	var (
		context internal.ClientContext
		client  sarama.Client
		admin   sarama.ClusterAdmin
		err     error
		exists  bool
		t       Topic
	)

	if context, err = internal.CreateClientContext(); err != nil {
		return err
	}

	if client, err = internal.CreateClient(&context); err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	if exists, err = internal.TopicExists(&client, topic); err != nil {
		return errors.Wrap(err, "failed to read topics")
	}

	if !exists {
		return errors.Errorf("topic '%s' does not exist", topic)
	}

	if admin, err = internal.CreateClusterAdmin(&context); err != nil {
		return errors.Wrap(err, "failed to create cluster admin")
	}

	fields := allFields
	fields.config = flags.PrintConfigs

	if t, err = readTopic(&client, &admin, topic, fields); err != nil {
		return errors.Wrap(err, "failed to read topic")
	}

	return operation.printTopic(t, flags)
}

func (operation *Operation) printTopic(topic Topic, flags DescribeTopicFlags) error {

}

func readTopic(client *sarama.Client, admin *sarama.ClusterAdmin, name string, requestedFields requestedTopicFields) (Topic, error) {
	var (
		err error
		ps  []int32
		led *sarama.Broker
		top = Topic{Name: name}
	)

	if !requestedFields.partitionID {
		return top, nil
	}

	if ps, err = (*client).Partitions(name); err != nil {
		return top, err
	}

	partitionChannel := make(chan Partition)

	// read partitions in parallel
	for _, p := range ps {
		go func(partitionId int32) {
			
		}()
	}
}
