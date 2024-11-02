package acl

type ResourceACLEntry struct {
	ResourceType string  `json:"resourceType" yaml:"resourceType"`
	ResourceName string  `json:"resourceName" yaml:"resourceName"`
	PatternType  string  `json:"patternType" yaml:"patternType"`
	Acls         []Entry `json:"acls" yaml:"acls"`
}

type Entry struct {
	Principal      string
	Host           string
	Operation      string
	PermissionType string `json:"permissionType" yaml:"permissionType"`
}

type GetACLFlags struct {
	OutputFormat string
	FilterTopic  string
	Operation    string
	PatternType  string
	Allow        string
	Deny         string
	Topics       string
	Groups       string
	Cluster      string
}

type CreateACLFlags struct {
	Principal    string
	Hosts        []string
	Operations   []string
	Allow        bool
	Deny         bool
	Topic        string
	Group        string
	Cluster      string
	PatternType  string
	ValidateOnly bool
}

type DeleteACLFlags struct {
	ValidateOnly bool
	Topics       bool
	Groups       bool
	Cluster      bool
	Allow        bool
	Deny         bool
	Operation    string
	PatternType  string
}

type Operation struct {
}

func (operation *Operation) GetACL(flags GetACLFlags) error {
	var (
	//ctx internal.Cl
	)
}
