package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	gossip "github.com/j-mroz/gossip-cluster/proto/gossip/v1"
)

const (
	heartbeatTime  = 2 * time.Second
	connectTimeout = 2 * time.Second
)

// Node represents cluster node.
type Node struct {
	GossipServer

	members *MembersList
	self    *Member
}

// NewNode creates new Node instance
func NewNode(name string, port uint16) *Node {
	self := &Member{
		Name:      name,
		Port:      port,
		StartTime: time.Now().Unix(),
	}

	node := &Node{
		members: NewMembersList(),
		self:    self,
	}
	node.GossipServer = GossipServer{node}

	go node.runHeartbeatLoop()
	return node
}

// Should be called every time we modify our cluster state
func (n *Node) advanceIndex() {
	atomic.AddUint64(&n.self.Index, 1)
}

func (n *Node) advanceHeartbeat() {
	atomic.AddUint64(&n.self.Heartbeat, 1)
}

func (n *Node) updateHeartbeat(newHeartbeat uint64) {
	for {
		currentHeartbeat := atomic.LoadUint64(&n.self.Heartbeat)
		if currentHeartbeat >= newHeartbeat {
			break
		}
		if atomic.CompareAndSwapUint64(&n.self.Heartbeat, currentHeartbeat, newHeartbeat) {
			break
		}
	}
}

func (n *Node) runHeartbeatLoop() {
	for {
		time.Sleep(heartbeatTime)
		log.Println("runHeartbeatLoop", atomic.LoadUint64(&n.self.Heartbeat))
		n.runHeartbeatActions()
	}
}

func (n *Node) runHeartbeatActions() {
	n.advanceHeartbeat()
	n.runGossipDisseminator()
	n.runFailureDetector()
}

func (n *Node) runFailureDetector() {
	// TODO
}

func (n *Node) runGossipDisseminator() {
	n.members.ReadTransaction(func(members []*Member) {
		n.sendGossips(members)
	})
}

func (n *Node) sendGossips(members []*Member) {
	update := n.makeMembersUpdate(members)
	gossipPeers := n.pickGossipGroup(members)

	for _, peerIndex := range gossipPeers {
		if members[peerIndex].Name == n.self.Name {
			continue
		}
		remote := members[peerIndex].Address()
		n.sendMembersUpdate(remote, update)
	}
}

func (n *Node) makeMembersUpdate(members []*Member) *gossip.MembersUpdate {
	update := &gossip.MembersUpdate{
		Name:    n.self.Name,
		Members: mapToGossipMembers(members),
	}
	update.Members = append(update.Members, mapToGossipMember(n.self))
	return update
}

func (n *Node) sendMembersUpdate(remote string, update *gossip.MembersUpdate) {
	n.connectAndDo(remote, func(remoteNode gossip.GossipClient) {
		remoteNode.PushMembersUpdate(context.Background(), update)
	})
}

func (n *Node) pickGossipGroup(members []*Member) []int {
	membersCount := len(members)
	if membersCount == 0 {
		return []int{}
	}

	indices := rand.Perm(membersCount)

	x := math.Log2(float64(membersCount))
	gossipRange := int(x)

	if gossipRange < membersCount {
		gossipRange++
	}

	return indices[:gossipRange]
}

func (n *Node) connectAndDo(remote string, op func(client gossip.GossipClient)) {
	remoteNode, conn, connErr := connectClient(remote)
	if connErr != nil {
		return
	}
	op(remoteNode)
	conn.Close()
}

func (n *Node) handlePullMembers(context.Context, *gossip.PullMembersRequest) (*gossip.PullMembersResponse, error) {
	var response *gossip.PullMembersResponse

	n.members.ReadTransaction(func(members []*Member) {
		response = &gossip.PullMembersResponse{
			Name:    n.self.Name,
			Members: mapToGossipMembers(members),
		}
	})

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
		Heartbeat: member.Heartbeat,
		Index:     member.Index,
		StartTime: member.StartTime,
	}
}

func mapToMember(gmember *gossip.Member) *Member {
	return &Member{
		Name:      gmember.Name,
		Host:      gmember.Host,
		Port:      uint16(gmember.Port),
		Heartbeat: gmember.Heartbeat,
		Index:     gmember.Index,
		StartTime: gmember.StartTime,
	}
}

func (n *Node) handlePushHeartbeat(ctx context.Context, heartbeat *gossip.Heartbeat) (*empty.Empty, error) {
	var txErr error

	n.members.UpdateTransaction(func(members *[]*Member) {
		_, member := findMember(*members, mapToMember(heartbeat.Originator))
		if member == nil {
			txErr = errors.New("disconnected from cluster")
		} else {
			member.Heartbeat = heartbeat.Originator.Heartbeat
			member.Stale = false
		}
	})

	return &empty.Empty{}, txErr
}

func (n *Node) handlePushMembersUpdate(ctx context.Context, update *gossip.MembersUpdate) (*empty.Empty, error) {
	n.updateHeartbeat(update.ClusterHeartbeat)
	n.updateMembers(update.GetMembers())
	return &empty.Empty{}, nil
}

func (n *Node) RequestJoin(remote string) {

	log.Println("Attempting to join cluster, connecting ", remote)

	n.connectAndDo(remote, func(remoteNode gossip.GossipClient) {
		request := &gossip.JoinRequest{
			Origiator: mapToGossipMember(n.self),
		}
		response, joinErr := remoteNode.Join(context.Background(), request)
		if joinErr != nil {
			log.Printf("Join error: %s\n", joinErr.Error())
			os.Exit(1)
		}
		logRequest(response)
		if response.Members == nil || len(response.Members) == 0 {
			log.Println("Join returned empty members list")
			os.Exit(1)
		}

		log.Println("Discovered self host: ", response.OriginatorHost)
		log.Printf("Drifted heartbeat from %v to %v", n.self.Heartbeat, response.ClusterHeartbeat)

		n.self.Host = response.OriginatorHost
		n.self.Index = response.ClusterHeartbeat

		n.updateMembers(response.GetMembers())

		log.Println("Connected ", remote, ", successfully joined cluster using node")
	})
}

func (n *Node) updateMembers(gmembers []*gossip.Member) {
	n.members.UpdateTransaction(func(members *[]*Member) {
		n.updateMembersImpl(members, gmembers)
	})
}

func (n *Node) updateMembersImpl(members *[]*Member, gmembers []*gossip.Member) {

	// Save references to original member in a map for faster lookups.
	membersMap := make(map[MemberKey]*Member)
	for _, member := range *members {
		key := MemberKey{Name: member.Name, Host: member.Host, Port: member.Port}
		membersMap[key] = member
	}

	for _, gmember := range gmembers {
		key := MemberKey{Name: gmember.Name, Host: gmember.Host, Port: uint16(gmember.Port)}
		if member, ok := membersMap[key]; ok {
			updateMember(member, gmember)
		} else if gmember.Name != n.self.Name {
			*members = append(*members, mapToMember(gmember))
		}
	}
}

func updateMember(member *Member, gmember *gossip.Member) {
	if member.Heartbeat <= gmember.Heartbeat {
		member.Heartbeat = gmember.Heartbeat
		member.Index = gmember.Index
		member.Stale = false
	} else {
		// TODO
	}
}

func connectClient(remote string) (client gossip.GossipClient, conn *grpc.ClientConn, err error) {
	timeContext, _ := context.WithTimeout(context.Background(), connectTimeout)
	conn, err = grpc.DialContext(timeContext, remote, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("grpc.Dial error: %v", err)
		return
	}
	client = gossip.NewGossipClient(conn)
	return
}

func (n *Node) handleJoin(ctx context.Context, request *gossip.JoinRequest) (*gossip.JoinResponse, error) {
	n.updateHeartbeat(request.Origiator.Heartbeat)
	newMember, err := n.makeNewMember(ctx, request)
	if err != nil {
		return nil, err
	}

	response := &gossip.JoinResponse{
		OriginatorHost:   newMember.Host,
		ClusterHeartbeat: n.self.Heartbeat,
	}

	n.members.UpdateTransaction(func(members *[]*Member) {
		response.Members = mapToGossipMembers(*members)
		*members = append(*members, newMember)
	})

	response.Members = append(response.Members, mapToGossipMember(n.self))

	return response, nil
}

func (n *Node) makeNewMember(ctx context.Context, request *gossip.JoinRequest) (*Member, error) {
	if request.Origiator == nil {
		return nil, errors.New("originator cannot be empty")
	}

	newMember := mapToMember(request.Origiator)
	peer, peerOk := peer.FromContext(ctx)
	if !peerOk {
		return nil, errors.New("refuse to join, peer not ok")
	}
	remoteAddr := peer.Addr.(*net.TCPAddr)
	newMember.Host = remoteAddr.IP.String()

	alreadyJoined := false
	n.members.ReadTransaction(func(members []*Member) {
		if _, oldMember := findMember(members, newMember); oldMember != nil {
			alreadyJoined = true
		}
	})
	if alreadyJoined {
		return nil, errors.New("already joined")
	}

	//TODO self check!

	return newMember, nil
}

func findMember(members []*Member, wanted *Member) (int, *Member) {
	for index, member := range members {
		if member.Host == wanted.Host && member.Port == wanted.Port && member.Name == wanted.Name {
			return index, member
		}
	}
	return -1, nil
}
