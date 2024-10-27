package auth

import (
	"github.com/hashicorp/go-plugin"
	"kafctl/pkg/plugins"
)

type AccessTokenProvider interface {
	Token() (string, error)
	Init(options map[string]any, brokers []string) error
}

var TokenProviderPluginSpec = plugins.PluginSpec[*TokenProviderPlugin, AccessTokenProvider]{
	PluginImpl:          &TokenProviderPlugin{},
	InterfaceIdentifier: "tokenProvider",
	Handshake: plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "KAFCTL_PLUGIN",
		MagicCookieValue: "TOKEN_PROVIDER_PLUGIN",
	},
}
