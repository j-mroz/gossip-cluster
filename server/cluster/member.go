package cluster

type Member struct {
	Name      string
	Address   string
	Index     uint64
	StartTime int64
	Stale     bool
}

type MemberKey struct {
	Name    string
	Address string
}
