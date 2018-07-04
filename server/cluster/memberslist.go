package cluster

import (
	"sync"
)

type MembersList struct {
	membersMu sync.RWMutex
	members   []*Member
}

func NewMembersList() *MembersList {
	return &MembersList{
		members: []*Member{},
	}
}

func (l *MembersList) ReadTransaction(tx func(members []*Member)) {
	l.membersMu.RLock()
	defer l.membersMu.RUnlock()
	tx(l.members)

}

func (l *MembersList) UpdateTransaction(tx func(members *[]*Member)) {
	l.membersMu.Lock()
	defer l.membersMu.Unlock()
	tx(&l.members)
}
