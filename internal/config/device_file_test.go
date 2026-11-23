package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func writeDeviceFile(t *testing.T, dir, content string) string {
	t.Helper()
	cfg := filepath.Join(dir, "config.yml")
	if err := os.WriteFile(DeviceConfigPath(cfg), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestDeviceFileTemplateIsValid(t *testing.T) {
	if k := CheckDeviceKeys([]byte(DeviceFileTemplate)); len(k) != 0 {
		t.Fatalf("unknown keys in template: %v", k)
	}
	var f DeviceFile
	if err := yaml.Unmarshal([]byte(DeviceFileTemplate), &f); err != nil {
		t.Fatal(err)
	}
	if f.Sync == nil || f.DeviceLimit == nil {
		t.Fatal("template misses a section")
	}
	if err := f.DeviceLimit.Validate(); err != nil {
		t.Fatal(err)
	}
	r := f.DeviceLimit.Runtime()
	if r.CountMethod != CountTraffic || !r.Merge || !r.Block || !r.LogOverLimit || r.RejectNewImmediately ||
		r.KickAfter.Minutes() != 5 || r.Watch.Minutes() != 10 || r.TCPKeepaliveSeconds != 180 ||
		!r.ClearPanelWhenUserIdle || r.MinTrafficBytes != 100*1024 || r.TrafficCheckSeconds != 60 {
		t.Fatalf("suggested values wrong: %+v", r)
	}
	if !f.Sync.RESTPolling() {
		t.Fatal("rest polling should be on")
	}
}

func TestApplyDeviceFileMissingChangesNothing(t *testing.T) {
	dir := t.TempDir()
	rc := &RootConfig{}
	rc.Config.DeviceLimit = &DeviceLimitConfig{CountMethod: CountOff}
	used, problems := rc.ApplyDeviceFile(filepath.Join(dir, "config.yml"))
	if used || len(problems) != 0 {
		t.Fatalf("used=%v problems=%v", used, problems)
	}
	if rc.Config.DeviceLimit.CountMethod != CountOff {
		t.Fatal("top-level block changed without a file")
	}
}

func TestApplyDeviceFileOverridesTopLevelAndIsInherited(t *testing.T) {
	dir := t.TempDir()
	cfg := writeDeviceFile(t, dir, "device_limit:\n    count_method: traffic\n    ipv4_ipv6_counting: merge\n")
	own := &DeviceLimitConfig{CountMethod: CountOff}
	rc := &RootConfig{Instances: []Config{{}, {DeviceLimit: own}}}
	rc.Config.DeviceLimit = &DeviceLimitConfig{CountMethod: CountRecentConnection}

	used, problems := rc.ApplyDeviceFile(cfg)
	if !used || len(problems) != 0 {
		t.Fatalf("used=%v problems=%v", used, problems)
	}
	for i := range rc.Instances {
		rc.Instances[i].inheritFrom(&rc.Config)
	}
	if got := rc.Instances[0].DeviceLimit.Runtime(); got.CountMethod != CountTraffic || !got.Merge {
		t.Fatalf("instance without its own block should use the file: %+v", got)
	}
	if rc.Instances[1].DeviceLimit != own {
		t.Fatal("an instance's own block must win over the file")
	}
}

func TestApplyDeviceFileInvalidBlockIgnored(t *testing.T) {
	dir := t.TempDir()
	cfg := writeDeviceFile(t, dir, "device_limit:\n    count_method: bogus\nsync:\n    rest_polling_with_websocket: false\n")
	rc := &RootConfig{}
	used, problems := rc.ApplyDeviceFile(cfg)
	if !used || len(problems) != 1 || !strings.Contains(problems[0], "device_limit ignored") {
		t.Fatalf("used=%v problems=%v", used, problems)
	}
	if rc.Config.DeviceLimit != nil {
		t.Fatal("invalid block must not be applied")
	}
	if rc.Config.Sync == nil || rc.Config.Sync.RESTPolling() {
		t.Fatal("the valid sync block should still apply")
	}
}

func TestApplyDeviceFileBadYAMLAndUnknownKey(t *testing.T) {
	dir := t.TempDir()
	cfg := writeDeviceFile(t, dir, "device_limit: [1,\n")
	rc := &RootConfig{}
	used, problems := rc.ApplyDeviceFile(cfg)
	if used || len(problems) != 1 || rc.Config.DeviceLimit != nil {
		t.Fatalf("used=%v problems=%v", used, problems)
	}

	cfg = writeDeviceFile(t, dir, "device_limit:\n    count_methd: traffic\n    block_devices: false\n")
	rc = &RootConfig{}
	used, problems = rc.ApplyDeviceFile(cfg)
	if !used || len(problems) != 1 || !strings.Contains(problems[0], "unknown key") {
		t.Fatalf("used=%v problems=%v", used, problems)
	}
	if rc.Config.DeviceLimit == nil || rc.Config.DeviceLimit.Runtime().Block {
		t.Fatal("known keys should still apply")
	}
}

func TestEnableDisableDeviceFile(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yml")
	path := DeviceConfigPath(cfg)

	if _, action, err := DisableDeviceFile(cfg); err != nil || action != "not enabled" {
		t.Fatalf("disable without file: %q %v", action, err)
	}

	p, action, err := EnableDeviceFile(cfg)
	if err != nil || p != path || !strings.HasPrefix(action, "created") {
		t.Fatalf("enable: %q %v", action, err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != DeviceFileTemplate {
		t.Fatalf("file content wrong: %v", err)
	}
	if st, _ := os.Stat(path); st.Mode().Perm() != 0o644 {
		t.Fatalf("mode %v", st.Mode().Perm())
	}
	if _, action, _ := EnableDeviceFile(cfg); action != "already enabled" {
		t.Fatalf("second enable: %q", action)
	}

	// user edit survives disable + enable
	edited := strings.Replace(DeviceFileTemplate, "min_traffic_kb: 100", "min_traffic_kb: 250", 1)
	if err := os.WriteFile(path, []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, action, err := DisableDeviceFile(cfg); err != nil || action != "moved to device_config.yml.off" {
		t.Fatalf("disable: %q %v", action, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file still present after disable")
	}
	if _, action, err := EnableDeviceFile(cfg); err != nil || !strings.HasPrefix(action, "restored") {
		t.Fatalf("enable after disable: %q %v", action, err)
	}
	if got, _ := os.ReadFile(path); string(got) != edited {
		t.Fatal("edits lost")
	}

	// a saved copy is never overwritten
	if err := os.WriteFile(path+".off", []byte("old: copy\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := DisableDeviceFile(cfg); err != nil {
		t.Fatal(err)
	}
	matches, _ := filepath.Glob(path + ".off*")
	if len(matches) != 2 {
		t.Fatalf("expected the old copy to be kept under a timestamp, got %v", matches)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
}
