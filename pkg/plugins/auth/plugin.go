package auth

import (
	"github.com/hashicorp/go-plugin"
	"net/rpc"
)

type TokenProviderPlugin struct {
	Impl AccessTokenProvider
}

func (p *TokenProviderPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &TokenProviderRPCServer{Impl: p.Impl}, nil
}

func (TokenProviderPlugin) Client(_ *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &TokenProviderRPC{client: c}, nil
}

// TokenProviderRPC is the rpc implementation i.e. the one that is used by kafctl
type TokenProviderRPC struct {
	client *rpc.Client
}

func (g *TokenProviderRPC) Token() (string, error) {
	var resp string
	err := g.client.Call("Plugin.Token", new(interface{}), &resp)
	return resp, err
}

func (g *TokenProviderRPC) Init(options map[string]any, brokers []string) error {
	var resp interface{}

	args := options
	args["brokers"] = brokers

	for key, value := range options {
		args[key] = value
	}

	return g.client.Call("Plugin.Init", args, &resp)
}

type TokenProviderRPCServer struct {
	Impl AccessTokenProvider
}

func (s *TokenProviderRPCServer) Token(_ interface{}, resp *string) error {
	v, err := s.Impl.Token()
	*resp = v
	return err
}

func (s *TokenProviderRPCServer) Init(args map[string]interface{}, _ *interface{}) error {
	brokers := args["brokers"].([]string)
	delete(args, "brokers")

	return s.Impl.Init(args, brokers)
}
