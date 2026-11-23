package singbox

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/panel"
)

func TestBuildInbound_TCPKeepAlive(t *testing.T) {
	for _, protocol := range []string{"shadowsocks", "socks"} {
		nc := &panel.NodeConfig{Protocol: protocol, ServerPort: 111, Cipher: "aes-128-gcm"}
		inbound := buildInbound(testNodeSpec(nc), testUsers, kernel.TLSCert{})
		if inbound == nil {
			t.Fatalf("%s: inbound is nil", protocol)
		}
		assertMapValue(t, inbound, "tcp_keep_alive", "180s")
		assertMapValue(t, inbound, "tcp_keep_alive_interval", "30s")
	}
}

func TestApplyTCPKeepalive(t *testing.T) {
	in := M{"tcp_keep_alive": "180s", "tcp_keep_alive_interval": "30s"}
	applyTCPKeepalive(in, 0)
	if _, ok := in["tcp_keep_alive"]; ok {
		t.Fatal("keepalive not removed for 0")
	}
	applyTCPKeepalive(in, 600)
	if in["tcp_keep_alive"] != "600s" || in["tcp_keep_alive_interval"] != "30s" {
		t.Fatalf("got %v", in)
	}
	applyTCPKeepalive(in, 20)
	if in["tcp_keep_alive"] != "20s" || in["tcp_keep_alive_interval"] != "20s" {
		t.Fatalf("got %v", in)
	}
}
