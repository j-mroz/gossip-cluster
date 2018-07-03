package cluster

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"fmt"
	"os"

	"github.com/golang/protobuf/ptypes/empty"
	gossip "github.com/j-mroz/gossip-cluster/proto/gossip/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

const heartbeatTime = 2 * time.Second

// Node represents cluster node.
type Node struct {
	GossipServer

	membersMu sync.RWMutex
	members   []*Member
	self      *Member
}

// NewNode creates new Node instance
func NewNode(name string, port uint16) *Node {
	self := &Member{
		Name:      name,
		Port:      port,
		StartTime: time.Now().Unix(),
	}
	node := &Node{
		self:    self,
		members: make([]*Member, 0, 10),
	}
	node.members = append(node.members, self)
	node.GossipServer = GossipServer{node}
	go node.runHeartbeatLoop()
	return node
}

func (n *Node) runHeartbeatLoop() {
	for {
		time.Sleep(heartbeatTime)
		n.self.Heartbeat++
		msg := &gossip.Heartbeat{Originator: mapToGossipMember(n.self)}
		for _, member := range n.members {
			if member == n.self {
				continue
			}
			remoteNode, conn, connErr := connectClient(member.Address())
			if connErr != nil {
				return
			}
			remoteNode.PushHeartbeat(context.Background(), msg)
			conn.Close()
		}
	}
}

func (n *Node) runFailureDetectorLoop() {
	// TODO
}

func (n *Node) runGossipDisseminator() {
	// TODO
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
		Host:      member.Host,
		Port:      int32(member.Port),
		Index:     member.Index,
		StartTime: member.StartTime,
	}
}

func mapToMember(gmember *gossip.Member) *Member {
	return &Member{
		Name:      gmember.Name,
		Host:      gmember.Host,
		Port:      uint16(gmember.Port),
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
		key := MemberKey{Name: member.Name, Host: member.Host, Port: member.Port}
		membersMap[key] = member
	}

	for _, gmember := range gmembers {
		key := MemberKey{Name: gmember.Name, Host: gmember.Host, Port: uint16(gmember.Port)}
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

		// TODO
	}
}

func (n *Node) handlePushHeartbeat(ctx context.Context, heartbeat *gossip.Heartbeat) (*empty.Empty, error) {
	_, member := n.findMember(mapToMember(heartbeat.Originator))
	if member == nil {
		return nil, errors.New("disconnected from cluster")
	}
	member.Heartbeat = heartbeat.Originator.Heartbeat
	member.Stale = false
	return &empty.Empty{}, nil
}

func (n *Node) handlePushMembersUpdate(context.Context, *gossip.MembersUpdate) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (n *Node) RequestJoin(remote string) {

	log.Println("Attempting to join cluster, connecting ", remote)

	remoteNode, conn, connErr := connectClient(remote)
	if connErr != nil {
		return
	}
	defer conn.Close()

	request := &gossip.JoinRequest{
		Origiator: mapToGossipMember(n.self),
	}
	response, joinErr := remoteNode.Join(context.Background(), request)
	if joinErr != nil {
		fmt.Printf("join error: %s\n", joinErr.Error())
		os.Exit(1)
	}
	logRequest(response)

	log.Println("Discovered self host: ", response.OriginatorHost)
	log.Printf("Drifted index from %v to %v", n.self.Index, response.ClusterIndex)

	n.self.Host = response.OriginatorHost
	n.self.Index = response.ClusterIndex

	n.updateMembers(response.GetMembers())

	log.Println("Connected ", remote, ", successfully joined cluster using node")
}

func connectClient(remote string) (client gossip.GossipClient, conn *grpc.ClientConn, err error) {
	conn, err = grpc.Dial(remote, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("grpc.Dial error: %v", err)
		return
	}
	client = gossip.NewGossipClient(conn)
	return
}

func (n *Node) handleJoin(ctx context.Context, request *gossip.JoinRequest) (*gossip.JoinResponse, error) {

	if request.Origiator == nil {
		return nil, errors.New("Originator cannot be empty")
	}

	newMember := mapToMember(request.Origiator)
	peer, peerOk := peer.FromContext(ctx)
	if !peerOk {
		return nil, errors.New("refuse to join, peer not ok")
	}
	remoteAddr := peer.Addr.(*net.TCPAddr)
	newMember.Host = remoteAddr.IP.String()

	if _, oldMember := n.findMember(newMember); oldMember != nil {
		return nil, errors.New("already joined")
	}

	response := &gossip.JoinResponse{
		OriginatorHost: newMember.Host,
		ClusterIndex:   n.self.Index,
		Members:        mapToGossipMembers(n.members),
	}

	n.addMember(newMember)

	return response, nil
}

func (n *Node) addMember(newMember *Member) {
	n.membersMu.Lock()
	defer n.membersMu.Unlock()
	n.members = append(n.members, newMember)
}

func (n *Node) findMember(wanted *Member) (int, *Member) {
	n.membersMu.RLock()
	defer n.membersMu.RUnlock()
	return findMember(n.members, wanted)
}

func findMember(members []*Member, wanted *Member) (int, *Member) {
	for index, member := range members {
		if member.Host == wanted.Host && member.Port == wanted.Port && member.Name == wanted.Name {
			return index, member
		}
	}
	return -1, nil
}
