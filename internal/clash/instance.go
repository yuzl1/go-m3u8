package clash

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ExtractNode returns the YAML block of the proxy entry named nodeName
// from a clash config (preserving the original node definition).
func ExtractNode(yamlText, nodeName string) (string, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(yamlText), &doc); err != nil {
		return "", fmt.Errorf("parse clash yaml: %w", err)
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return "", fmt.Errorf("invalid clash config")
	}
	root := doc.Content[0]
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "proxies" {
			continue
		}
		seq := root.Content[i+1]
		if seq.Kind != yaml.SequenceNode {
			return "", fmt.Errorf("clash proxies section is not a list")
		}
		for _, item := range seq.Content {
			if mappingValue(item, "name") == nodeName {
				out, err := yaml.Marshal(item)
				if err != nil {
					return "", err
				}
				return strings.TrimRight(string(out), "\n"), nil
			}
		}
	}
	return "", fmt.Errorf("node %q not found in clash config", nodeName)
}

// ParseProxyNames extracts the names of all entries in the `proxies:`
// section of a clash config — the REAL nodes, excluding proxy-groups.
func ParseProxyNames(yamlText string) ([]string, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(yamlText), &doc); err != nil {
		return nil, fmt.Errorf("parse clash yaml: %w", err)
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("invalid clash config")
	}
	root := doc.Content[0]
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "proxies" {
			continue
		}
		seq := root.Content[i+1]
		if seq.Kind != yaml.SequenceNode {
			return nil, fmt.Errorf("clash proxies section is not a list")
		}
		names := make([]string, 0, len(seq.Content))
		for _, item := range seq.Content {
			if n := mappingValue(item, "name"); n != "" {
				names = append(names, n)
			}
		}
		return names, nil
	}
	return nil, fmt.Errorf("no proxies section found in clash config")
}

func mappingValue(node *yaml.Node, key string) string {
	if node == nil || node.Kind != yaml.MappingNode {
		return ""
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1].Value
		}
	}
	return ""
}

// BuildInstanceConfig generates a minimal per-task mihomo config that
// routes ALL traffic through a single node.
func BuildInstanceConfig(nodeName, nodeBlock string, port int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "mixed-port: %d\n", port)
	b.WriteString("allow-lan: false\n")
	b.WriteString("mode: rule\n")
	b.WriteString("log-level: warning\n")
	b.WriteString("geo-auto-update: false\n") // no geo DB download at startup
	b.WriteString("proxies:\n")
	lines := strings.Split(nodeBlock, "\n")
	for i, line := range lines {
		if i == 0 {
			b.WriteString("  - ")
			b.WriteString(line)
			b.WriteString("\n")
		} else {
			b.WriteString("    ")
			b.WriteString(line)
			b.WriteString("\n")
		}
	}
	b.WriteString("proxy-groups:\n")
	b.WriteString("  - name: TASK\n")
	b.WriteString("    type: select\n")
	fmt.Fprintf(&b, "    proxies: [%s]\n", nodeName)
	b.WriteString("rules:\n")
	b.WriteString("  - MATCH,TASK\n")
	return b.String()
}

// Instance wraps a running per-task mihomo process plus its temp dir so
// callers can clean up both.
type Instance struct {
	Cmd  *exec.Cmd
	Dir  string     // temp dir holding the generated config (remove on stop)
	done chan error // filled when the process exits
}

// Stop kills the process and removes its temp dir.
func (i *Instance) Stop() {
	if i == nil {
		return
	}
	if i.Cmd != nil && i.Cmd.Process != nil {
		i.Cmd.Process.Kill()
		<-i.done // reap
	}
	if i.Dir != "" {
		os.RemoveAll(i.Dir)
	}
}

// StartInstance launches a dedicated mihomo process for one node,
// listening on the given port. Returns the instance once the proxy port
// accepts connections.
func StartInstance(nodeName, nodeBlock string, port int) (*Instance, error) {
	bin, err := exec.LookPath("mihomo")
	if err != nil {
		return nil, fmt.Errorf("mihomo binary not found: %w", err)
	}
	dir, err := os.MkdirTemp("", "clash-task-")
	if err != nil {
		return nil, err
	}
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(BuildInstanceConfig(nodeName, nodeBlock, port)), 0644); err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, "mihomo.log")
	logF, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin, "-d", dir, "-f", cfgPath)
	cmd.Stdout = logF
	cmd.Stderr = logF
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	// Wait until the proxy port accepts connections (or the process dies).
	deadline := time.Now().Add(15 * time.Second)
	for {
		select {
		case werr := <-done:
			logTail := readLogTail(logPath)
			return nil, fmt.Errorf("mihomo exited during startup: %v\n%s", werr, logTail)
		default:
		}
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return &Instance{Cmd: cmd, Dir: dir, done: done}, nil
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			<-done
			logTail := readLogTail(logPath)
			return nil, fmt.Errorf("mihomo instance on port %d did not start in time\n%s", port, logTail)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// readLogTail returns the last ~2KB of a log file.
func readLogTail(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	if len(data) > 2048 {
		data = data[len(data)-2048:]
	}
	return strings.TrimSpace(string(data))
}
