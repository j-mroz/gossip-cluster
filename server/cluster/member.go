package cluster

import "fmt"

type Member struct {
	Name      string
	Host      string
	Port      uint16
	Index     uint64
	Timestamp int64
	StartTime int64
	Stale     bool
}

type MemberKey struct {
	Name string
}

func (m *Member) Address() string {
	return fmt.Sprintf("%s:%d", m.Host, m.Port)
}
