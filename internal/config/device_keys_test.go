package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDeviceLimitYAML(t *testing.T) {
	const doc = `
panel: {url: "https://p.example", token: "t", node_id: 1}
sync:
  rest_polling_with_websocket: false
device_limit:
  count_method: traffic
  min_traffic_kb: 250
  ipv4_ipv6_counting: merge
  block_devices: true
  over_limit_kick_after_minutes: 0
  ignore_ips: ["10.0.0.0/8"]
`
	var c Config
	if err := yaml.Unmarshal([]byte(doc), &c); err != nil {
		t.Fatal(err)
	}
	if c.Sync.RESTPolling() {
		t.Error("sync.rest_polling_with_websocket: false not read")
	}
	r := c.DeviceLimit.Runtime()
	if r.CountMethod != "traffic" || r.MinTrafficBytes != 250*1024 || !r.Merge || !r.Block || r.KickAfter != 0 || len(r.IgnorePrefixes) != 1 {
		t.Errorf("unexpected runtime %+v", r)
	}
	if msgs := CheckDeviceKeys([]byte(doc)); len(msgs) != 0 {
		t.Errorf("valid config reported unknown keys: %v", msgs)
	}
}

func TestCheckDeviceKeysFlagsTypos(t *testing.T) {
	const doc = `
device_limit:
  block_device: true
sync:
  rest_polling: false
instances:
  - device_limit:
      count_methd: traffic
`
	msgs := strings.Join(CheckDeviceKeys([]byte(doc)), "\n")
	for _, want := range []string{"block_device", "rest_polling", "count_methd", "instances[0]"} {
		if !strings.Contains(msgs, want) {
			t.Errorf("missing %q in:\n%s", want, msgs)
		}
	}
}

func TestInstancesInheritDeviceLimit(t *testing.T) {
	parent := &Config{DeviceLimit: &DeviceLimitConfig{IPv4IPv6Counting: "merge"}, Sync: &SyncConfig{}}
	own := &Config{DeviceLimit: &DeviceLimitConfig{CountMethod: "off"}}
	inherit := &Config{}
	own.inheritFrom(parent)
	inherit.inheritFrom(parent)
	if own.DeviceLimit.CountMethod != "off" {
		t.Error("instance override replaced by the parent")
	}
	if inherit.DeviceLimit == nil || !inherit.DeviceLimit.Runtime().Merge {
		t.Error("instance did not inherit device_limit")
	}
	if inherit.Sync == nil {
		t.Error("instance did not inherit sync")
	}
}
