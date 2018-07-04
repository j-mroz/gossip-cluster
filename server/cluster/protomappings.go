package cluster

import gossip "github.com/j-mroz/gossip-cluster/proto/gossip/v1"

func mapToGossipMembers(members []*Member) (gossipMembers []*gossip.Member) {
	gossipMembers = make([]*gossip.Member, 0, len(members))
	for _, member := range members {
		gossipMembers = append(gossipMembers, mapToGossipMember(member))
	}
	return gossipMembers
}

func filterAndMapToGossipMembers(members []*Member, filter func(*Member) bool) (gossipMembers []*gossip.Member) {
	gossipMembers = make([]*gossip.Member, 0, len(members))
	for _, member := range members {
		if filter(member) {
			gossipMembers = append(gossipMembers, mapToGossipMember(member))
		}
	}
	return gossipMembers
}

func mapToGossipMember(member *Member) *gossip.Member {
	return &gossip.Member{
		Name:      member.Name,
		Host:      member.Host,
		Port:      int32(member.Port),
		Timestamp: member.Timestamp,
		Index:     member.Index,
		StartTime: member.StartTime,
	}
}

func mapToMember(gmember *gossip.Member) *Member {
	return &Member{
		Name:      gmember.Name,
		Host:      gmember.Host,
		Port:      uint16(gmember.Port),
		Timestamp: gmember.Timestamp,
		Index:     gmember.Index,
		StartTime: gmember.StartTime,
	}
}
