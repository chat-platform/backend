package ids

// Typed string IDs shared across service boundaries (gRPC, WS, SQL).
// Domain models in each service should wrap these rather than invent parallel types.

type UserID string
type DeviceID string
type ChatID string
type ClientMsgID string
type ConnID string
type NodeID string

func (id UserID) String() string      { return string(id) }
func (id DeviceID) String() string    { return string(id) }
func (id ChatID) String() string      { return string(id) }
func (id ClientMsgID) String() string { return string(id) }
func (id ConnID) String() string      { return string(id) }
func (id NodeID) String() string      { return string(id) }
