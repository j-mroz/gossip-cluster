package cluster

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"fmt"
	"os"

	"github.com/golang/protobuf/ptypes/empty"
	gossip "github.com/j-mroz/gossip-cluster/proto/gossip/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// Node represents cluster node.
// Implements gossip.v1 proto.
type Node struct {
	GossipServer

	membersMu sync.RWMutex
	members   []*Member
	self      *Member
}

// NewNode creates new Node instance
func NewNode(name string) *Node {
	self := &Member{
		Name:      name,
		StartTime: time.Now().Unix(),
	}
	node := &Node{
		self:    self,
		members: make([]*Member, 0, 10),
	}
	node.members = append(node.members, self)
	node.GossipServer = GossipServer{node}
	return node
}

// Should be called every time we modify our cluster state
func (n *Node) advanceIndex() {
	n.self.Index++
}

func (n *Node) handlePullMembers(context.Context, *gossip.PullMembersRequest) (*gossip.PullMembersResponse, error) {

	response := &gossip.PullMembersResponse{
		Name:    n.self.Name,
		Members: mapToGossipMembers(n.members),
	}

	return response, nil
}

func mapToGossipMembers(members []*Member) (gossipMembers []*gossip.Member) {
	gossipMembers = make([]*gossip.Member, 0, len(members))
	for _, member := range members {
		gossipMembers = append(gossipMembers, mapToGossipMember(member))
	}
	return gossipMembers
}

func mapToGossipMember(member *Member) *gossip.Member {
	return &gossip.Member{
		Name:      member.Name,
		Address:   member.Address,
		Index:     member.Index,
		StartTime: member.StartTime,
	}
}

func mapToMember(gmember *gossip.Member) *Member {
	return &Member{
		Name:      gmember.Name,
		Address:   gmember.Address,
		Index:     gmember.Index,
		StartTime: gmember.StartTime,
	}
}

func (n *Node) updateMembers(gmembers []*gossip.Member) {
	n.membersMu.Lock()
	n.membersMu.Unlock()

	// Save references to original member in a map
	membersMap := make(map[MemberKey]*Member)
	for _, member := range n.members {
		key := MemberKey{Name: member.Name, Address: member.Address}
		membersMap[key] = member
	}

	for _, gmember := range gmembers {
		key := MemberKey{Name: gmember.Name, Address: gmember.Address}
		if member, ok := membersMap[key]; ok {
			n.updateMember(member, gmember)
		} else {
			n.members = append(n.members, mapToMember(gmember))
		}
	}
}

func (n *Node) updateMember(member *Member, gmember *gossip.Member) {
	if member.Index <= gmember.Index {
		member.Index = gmember.Index
		member.Stale = false
	} else {

	}
}

func (n *Node) handlePushHeartbeat(ctx context.Context, heartbeat *gossip.Heartbeat) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (n *Node) handlePushMembersUpdate(context.Context, *gossip.MembersUpdate) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (n *Node) RequestJoin(remote string) {

	log.Println("Attempting to join cluster using node", remote)

	conn, err := grpc.Dial(remote, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("grpc.Dial error: %v", err)
		return
	}
	defer conn.Close()

	remoteNode := gossip.NewGossipClient(conn)
	request := &gossip.JoinRequest{
		Origiator: mapToGossipMember(n.self),
	}
	response, joinErr := remoteNode.Join(context.Background(), request)
	if joinErr != nil {
		fmt.Printf("join error: %s\n", joinErr.Error())
		os.Exit(1)
	}
	logRequest(response)

	n.self.Address = response.OriginatorAddres
	log.Println("Discovered self address: ", n.self.Address)

	log.Printf("Drifted index from %v to %v", n.self.Index, response.ClusterIndex)
	n.self.Index = response.ClusterIndex

	n.updateMembers(response.GetMembers())

	log.Println("Successfully joined cluster using node ", remote)
}

func (n *Node) handleJoin(ctx context.Context, request *gossip.JoinRequest) (*gossip.JoinResponse, error) {

	if request.Origiator == nil {
		return nil, errors.New("Originator cannot be empty")
	}

	_, joinMember := findMember(n.members, mapToMember(request.Origiator))
	if joinMember != nil {
		return nil, errors.New("Already joined")
	}

	peer, peerOk := peer.FromContext(ctx)
	if !peerOk {
		return nil, errors.New("refuse to join, peer not ok")
	}

	response := &gossip.JoinResponse{
		OriginatorAddres: peer.Addr.String(),
		ClusterIndex:     n.self.Index,
		Members:          mapToGossipMembers(n.members),
	}

	return response, nil
}

func findMember(members []*Member, wanted *Member) (int, *Member) {
	for index, member := range members {
		if member.Address == wanted.Address && member.Name == wanted.Name {
			return index, member
		}
	}
	return -1, nil
}
