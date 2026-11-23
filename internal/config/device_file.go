package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DeviceConfigFileName is the optional file, next to config.yml, that
// overrides the built-in sync / device-limit defaults. No file = defaults.
const DeviceConfigFileName = "device_config.yml"

// DeviceConfigPath returns where device_config.yml lives for a config path.
func DeviceConfigPath(configPath string) string {
	return filepath.Join(filepath.Dir(configPath), DeviceConfigFileName)
}

// DeviceFile is the content of device_config.yml.
type DeviceFile struct {
	Sync        *SyncConfig        `yaml:"sync"`
	DeviceLimit *DeviceLimitConfig `yaml:"device_limit"`
}

// ApplyDeviceFile layers device_config.yml (if present next to configPath)
// over the top-level sync: / device_limit: of rc, so every instance that has
// no block of its own inherits it. Order of precedence, strongest first:
// an instance's own block in config.yml, device_config.yml, a top-level block
// in config.yml, the built-in defaults.
//
// used reports whether the file existed and could be read as YAML. problems
// lists what was ignored or looks wrong (a block with an invalid value is
// ignored, so a typo never stops the node); it is for logging.
func (rc *RootConfig) ApplyDeviceFile(configPath string) (used bool, problems []string) {
	path := DeviceConfigPath(configPath)
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			problems = append(problems, fmt.Sprintf("%s ignored: %v", DeviceConfigFileName, err))
		}
		return false, problems
	}
	var f DeviceFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return false, append(problems, fmt.Sprintf("%s ignored, not valid YAML: %v", DeviceConfigFileName, strings.ReplaceAll(err.Error(), "\n", "; ")))
	}
	for _, msg := range CheckDeviceKeys(data) {
		problems = append(problems, fmt.Sprintf("%s: unknown key ignored: %s", DeviceConfigFileName, msg))
	}
	if f.Sync != nil {
		rc.Config.Sync = f.Sync
	}
	if f.DeviceLimit != nil {
		if err := f.DeviceLimit.Validate(); err != nil {
			problems = append(problems, fmt.Sprintf("%s: device_limit ignored, built-in defaults used: %v", DeviceConfigFileName, err))
		} else {
			rc.Config.DeviceLimit = f.DeviceLimit
		}
	}
	return true, problems
}

// EnableDeviceFile turns the override file on. It restores device_config.yml.off
// when there is one (the user's earlier edits), otherwise it writes a new file
// with the suggested values and a comment on every key. It never overwrites an
// existing device_config.yml.
func EnableDeviceFile(configPath string) (path, action string, err error) {
	path = DeviceConfigPath(configPath)
	if _, err := os.Stat(path); err == nil {
		return path, "already enabled", nil
	}
	off := path + ".off"
	if _, err := os.Stat(off); err == nil {
		if err := os.Rename(off, path); err != nil {
			return path, "", err
		}
		return path, "restored your saved " + DeviceConfigFileName + ".off", nil
	}
	if err := writeFileAtomic(path, []byte(DeviceFileTemplate), 0o644); err != nil {
		return path, "", err
	}
	return path, "created with the suggested values", nil
}

// DisableDeviceFile turns the override file off by renaming it to
// device_config.yml.off, so edits are kept. A saved copy already there is
// kept under a timestamped name, never overwritten.
func DisableDeviceFile(configPath string) (path, action string, err error) {
	path = DeviceConfigPath(configPath)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return path, "not enabled", nil
		}
		return path, "", err
	}
	off := path + ".off"
	if _, err := os.Stat(off); err == nil {
		kept := off + "." + time.Now().Format("20060102-150405")
		if err := os.Rename(off, kept); err != nil {
			return path, "", err
		}
	}
	if err := os.Rename(path, off); err != nil {
		return path, "", err
	}
	return path, "moved to " + filepath.Base(off), nil
}

// writeFileAtomic writes through a temp file in the same directory so the
// config watcher never sees a half-written file.
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".device-config-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Chmod(name, mode); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}
