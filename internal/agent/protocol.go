package agent

import "time"

// Message is the JSON control message exchanged over the agent WebSocket.
//
// Main -> Agent:  assign (new transfer), ping (keepalive)
// Agent -> Main:  hello (registration), pong, done, failed
type Message struct {
	Type        string        `json:"type"`
	AgentName   string        `json:"name,omitempty"`
	TransferID  string        `json:"transfer_id,omitempty"`
	Transfer    *TransferInfo `json:"transfer,omitempty"`
	Transferred int64         `json:"transferred,omitempty"`
	Error       string        `json:"error,omitempty"`
}

// TransferInfo describes a file the agent must pull.
type TransferInfo struct {
	ID       string `json:"id"`
	FileName string `json:"file_name"`
	Size     int64  `json:"size"`
}

// Agent represents a connected agent node.
type Agent struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Remote         string    `json:"remote"`
	Online         bool      `json:"online"`
	ConnectedAt    time.Time `json:"connected_at"`
	LastSeen       time.Time `json:"last_seen"`
	ActiveTransfer string    `json:"active_transfer,omitempty"`
}

// Transfer states: waiting -> transferring -> done / failed
type Transfer struct {
	ID          string    `json:"id"`
	TaskID      string    `json:"task_id"`
	FileName    string    `json:"file_name"`
	Size        int64     `json:"size"`
	Transferred int64     `json:"transferred"`
	Status      string    `json:"status"`
	AgentID     string    `json:"agent_id,omitempty"`
	AgentName   string    `json:"agent_name,omitempty"`
	Attempts    int       `json:"attempts"`
	Error       string    `json:"error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	DoneAt      time.Time `json:"done_at,omitempty"`
	FilePath    string    `json:"-"` // internal: absolute path on the main server
}
