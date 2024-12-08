package topic

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"kafctl/internal"
	"kafctl/internal/output"
	"kafctl/internal/util"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
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
	if flags.PrintConfigs == NoConfigs {
		topic.Configs = nil
	}

	if len(topic.Configs) != 0 && flags.OutputFormat != "json" && flags.OutputFormat != "yaml" {
		configTableWriter := output.CreateTableWriter()
		if err := configTableWriter.WriteHeader("CONFIG", "VALUE"); err != nil {
			return err
		}

		for _, c := range topic.Configs {
			if err := configTableWriter.Write(c.Name, c.Value); err != nil {
				return err
			}
		}

		if err := configTableWriter.Flush(); err != nil {
			return err
		}
		output.PrintStrings("")
	}

	if flags.SkipEmptyPartitions {
		partitionsWithMessages := make([]Partition, 0)
		for _, p := range topic.Partitions {
			if p.OldestOffset < p.NewestOffset {
				partitionsWithMessages = append(partitionsWithMessages, p)
			}
		}
		topic.Partitions = partitionsWithMessages
	}

	partitionTableWriter := output.CreateTableWriter()

	if flags.OutputFormat == "" || flags.OutputFormat == "wide" {
		if err := partitionTableWriter.WriteHeader("PARTITION", "OLDEST_OFFSET", "NEWEST_OFFSET", "EMPTY",
			"LEADER", "REPLICAS", "IN_SYNC_REPLICAS"); err != nil {
			return err
		}
	} else if flags.OutputFormat != "json" && flags.OutputFormat != "yaml" {
		return errors.Errorf("unknown outputFormat: %s", flags.OutputFormat)
	}

	if flags.OutputFormat == "json" || flags.OutputFormat == "yaml" {
		return output.PrintObject(topic, flags.OutputFormat)
	} else if flags.OutputFormat == "wide" || flags.OutputFormat == "" {
		for _, p := range topic.Partitions {
			replicas := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(p.Replicas)), ","), "[]")
			inSyncReplicas := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(p.ISRs)), ","), "[]")
			if err := partitionTableWriter.Write(strconv.Itoa(int(p.ID)), strconv.Itoa(int(p.OldestOffset)),
				strconv.Itoa(int(p.NewestOffset)), strconv.FormatBool((p.NewestOffset-p.OldestOffset) <= 0), p.Leader, replicas, inSyncReplicas); err != nil {
				return err
			}
		}
	}

	if flags.OutputFormat == "" || flags.OutputFormat == "wide" {
		if err := partitionTableWriter.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (operation *Operation) AlterTopic(topic string, flags AlterTopicFlags) error {
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

	if t, err = readTopic(&client, &admin, topic, allFields); err != nil {
		return errors.Wrap(err, "failed to read topic")
	}

	if flags.Partitions != 0 {
		if len(t.Partitions) > int(flags.Partitions) {
			return errors.New("Decreasing the number of partitions is not supported")
		} else if len(t.Partitions) == int(flags.Partitions) {
			return errors.Errorf("Topic already has %d partitions", flags.Partitions)
		}

		if flags.ValidateOnly {
			for len(t.Partitions) < int(flags.Partitions) {
				t.Partitions = append(t.Partitions, Partition{ID: int32(len(t.Partitions)), NewestOffset: 0, OldestOffset: 0})
			}
		} else {
			var emptyAssignment = make([][]int32, 0)

			err = admin.CreatePartitions(topic, flags.Partitions, emptyAssignment, flags.ValidateOnly)
			if err != nil {
				return errors.Errorf("Could not create partitions for topic '%s': %v", topic, err)
			}
			output.Infof("paritions have been created")
		}
	}

	if flags.ReplicationFactor > 0 {
		var brokers = client.Brokers()

		if int(flags.ReplicationFactor) > len(brokers) {
			return errors.Errorf("Replication factor for topic '%s' must not exceed the number of available brokers", topic)
		}

		brokerReplicaCount := make(map[int32]int)
		for _, broker := range brokers {
			brokerReplicaCount[broker.ID()] = 0
		}

		for _, partition := range t.Partitions {
			for _, brokerID := range partition.Replicas {
				brokerReplicaCount[brokerID]++
			}
		}

		var replicaAssignment = make([][]int32, 0, int16(len(t.Partitions)))

		for _, partition := range t.Partitions {
			var replicas, err = getTargetReplicas(partition.Replicas, brokerReplicaCount, flags.ReplicationFactor)
			if err != nil {
				return errors.Wrap(err, "unable to determine target replicas")
			}
			replicaAssignment = append(replicaAssignment, replicas)
		}

		for brokerID, replicaCount := range brokerReplicaCount {
			output.Debugf("broker %d now has %d replicas", brokerID, replicaCount)
		}

		if flags.ValidateOnly {
			for i := range t.Partitions {
				t.Partitions[i].Replicas = replicaAssignment[i]
			}
		} else {
			err = admin.AlterPartitionReassignments(topic, replicaAssignment)
			if err != nil {
				return errors.Errorf("Could not reassign partition replicas for topic '%s': %v", topic, err)
			}

			partitions := make([]int32, len(t.Partitions))

			for _, p := range t.Partitions {
				partitions[0] = p.ID
			}

			assignmentRunning := true

			for assignmentRunning {
				status, err := admin.ListPartitionReassignments(topic, partitions)
				if err != nil {
					return errors.Errorf("Could not query reassignment status for topic '%s': %v", topic, err)
				}

				assignmentRunning = false

				if statusTopic, ok := status[topic]; ok {
					for partitionID, statusPartition := range statusTopic {
						output.Infof("reassignment running for topic=%s partition=%d: replicas:%v addingReplicas:%v removingReplicas:%v",
							topic, partitionID, statusPartition.Replicas, statusPartition.AddingReplicas, statusPartition.RemovingReplicas)
						time.Sleep(5 * time.Second)
						assignmentRunning = true
					}
				} else {
					output.Debugf("Empty list partition reassignment result returned (len status: %d)", len(status))
				}
			}

			output.Infof("partition replicas have been reassigned")
		}
	}

	if len(flags.Configs) > 0 {
		mergedConfigEntries := make(map[string]*string)

		for i, config := range t.Configs {
			mergedConfigEntries[config.Name] = &(t.Configs[i].Value)
		}

		for _, config := range flags.Configs {
			configParts := strings.Split(config, "=")

			if len(configParts) == 2 {
				if len(configParts[1]) == 0 {
					delete(mergedConfigEntries, configParts[0])
				} else {
					mergedConfigEntries[configParts[0]] = &configParts[1]
				}
			}
		}

		if flags.ValidateOnly {
			// validate only - directly alter the response object
			t.Configs = make([]internal.Config, 0, len(mergedConfigEntries))
			for key, value := range mergedConfigEntries {
				t.Configs = append(t.Configs, internal.Config{Name: key, Value: *value})
			}
		} else {
			if err = admin.AlterConfig(sarama.TopicResource, topic, mergedConfigEntries, flags.ValidateOnly); err != nil {
				return errors.Errorf("Could not alter topic config '%s': %v", topic, err)
			}
			output.Infof("config has ben altered")
		}
	}

	if flags.ValidateOnly {
		printConfigs := NoConfigs
		if len(flags.Configs) > 0 {
			printConfigs = NonDefaultConfigs
		}
		describeFlags := DescribeTopicFlags{PrintConfigs: printConfigs}
		return operation.printTopic(t, describeFlags)
	}

	return nil
}

func (operation *Operation) ListTopicsNames() ([]string, error) {

}

func getTargetReplicas(currentReplicas []int32, brokerReplicaCount map[int32]int, targetReplicationFactor int16) ([]int32, error) {
	replicas := currentReplicas

	for len(replicas) > int(targetReplicationFactor) {
		sort.Slice(replicas, func(i, j int) bool {
			brokerI := replicas[i]
			brokerJ := replicas[j]
			return brokerReplicaCount[brokerI] < brokerReplicaCount[brokerJ] || (brokerReplicaCount[brokerI] == brokerReplicaCount[brokerJ] && brokerI < brokerJ)
		})

		lastReplica := replicas[len(replicas)-1]
		replicas = replicas[:len(replicas)-1]
		brokerReplicaCount[lastReplica]--
	}

	var unusedBrokerIDs []int32

	if len(replicas) < int(targetReplicationFactor) {
		for brokerID := range brokerReplicaCount {
			if !util.ContainsInt32(replicas, brokerID) {
				unusedBrokerIDs = append(unusedBrokerIDs, brokerID)
			}
		}
		if len(unusedBrokerIDs) < (int(targetReplicationFactor) - len(replicas)) {
			return nil, errors.New("not enough brokers")
		}
	}

	for len(replicas) < int(targetReplicationFactor) {
		sort.Slice(unusedBrokerIDs, func(i, j int) bool {
			brokerI := unusedBrokerIDs[i]
			brokerJ := unusedBrokerIDs[j]
			return brokerReplicaCount[brokerI] < brokerReplicaCount[brokerJ] || (brokerReplicaCount[brokerI] == brokerReplicaCount[brokerJ] && brokerI > brokerJ)
		})

		replicas = append(replicas, unusedBrokerIDs[0])
		brokerReplicaCount[unusedBrokerIDs[0]]++
		unusedBrokerIDs = unusedBrokerIDs[1:]
	}

	return replicas, nil
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
			np := Partition{ID: partitionId}

			if requestedFields.partitionOffset {
				if np.OldestOffset, err = (*client).GetOffset(name, partitionId, sarama.OffsetOldest); err != nil {
					output.Warnf("unable to read oldest offset for topic %s partition %d", name, partitionId)
				}

				if np.NewestOffset, err = (*client).GetOffset(name, partitionId, sarama.OffsetNewest); err != nil {
					output.Warnf("unable to read newest offset for topic %s partition %d", name, partitionId)
				}
			}

			if requestedFields.partitionLeader {
				if led, err = (*client).Leader(name, partitionId); err != nil {
					output.Warnf("unable to read leader for topic %s partition %d", name, partitionId)
					np.Leader = "none"
				} else {
					np.Leader = led.Addr()
				}
			}

			if requestedFields.partitionReplicas {
				if np.Replicas, err = (*client).Replicas(name, partitionId); err != nil {
					output.Warnf("unable to read replicas for topic %s partition %d", name, partitionId)
				} else {
					sort.Slice(np.Replicas, func(i, j int) bool {
						return np.Replicas[i] < np.Replicas[j]
					})
				}
			}

			if requestedFields.partitionISRs {
				if np.ISRs, err = (*client).InSyncReplicas(name, partitionId); err != nil {
					output.Warnf("unable to read inSyncReplicas for topic %s partition %d", name, partitionId)
				} else {
					sort.Slice(np.ISRs, func(i, j int) bool {
						return np.ISRs[i] < np.ISRs[j]
					})
				}
			}

			partitionChannel <- np
		}(p)
	}

	for range ps {
		partition := <-partitionChannel
		top.Partitions = append(top.Partitions, partition)
	}

	sort.Slice(top.Partitions, func(i, j int) bool {
		return top.Partitions[i].ID < top.Partitions[j].ID
	})

	if requestedFields.config != NoConfigs {
		topicConfig := sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: name,
		}

		if top.Configs, err = internal.ListConfigs(admin, topicConfig, requestedFields.config == AllConfigs); err != nil {
			return top, err
		}
	}

	return top, nil
}
