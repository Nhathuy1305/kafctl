package acl

import (
	"fmt"
	"github.com/IBM/sarama"
	"testing"
)

func TestOperationFromString(t *testing.T) {
	tests := []sarama.AclOperation{
		sarama.AclOperationUnknown,
		sarama.AclOperationAny,
		sarama.AclOperationAll,
		sarama.AclOperationRead,
		sarama.AclOperationWrite,
		sarama.AclOperationCreate,
		sarama.AclOperationDelete,
		sarama.AclOperationAlter,
		sarama.AclOperationDescribe,
		sarama.AclOperationClusterAction,
		sarama.AclOperationDescribeConfigs,
		sarama.AclOperationAlterConfigs,
		sarama.AclOperationIdempotentWrite,
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("test operation %d", tt), func(t *testing.T) {
			//stringOperation := operationT
		})
	}
}
