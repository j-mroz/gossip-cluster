package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	gossip "github.com/j-mroz/gossip-cluster/proto/gossip/v1"
)

const (
	heartbeatTime       = 2 * time.Second
	connectTimeout      = 3 * time.Second
	memberFailTimeout   = 8 * time.Second
	memberRemoveTimeout = 24 * time.Second
)

// Node represents cluster node.
type Node struct {
	GossipServer

	members *MembersList
	self    *Member
}

// NewNode creates new Node instance
func NewNode(name string, host string, port uint16) *Node {
	self := &Member{
		Name:      name,
		Host:      host,
		Port:      port,
		Timestamp: time.Now().UnixNano(),
		StartTime: time.Now().UnixNano(),
	}

	node := &Node{
		members: NewMembersList(),
		self:    self,
	}
	node.GossipServer = GossipServer{node}

	node.members.UpdateTransaction(func(members *[]*Member) {
		node.addMember(members, self)
	})

	go node.runHeartbeatLoop()
	return node
}

// Should be called every time we modify our cluster state
func (n *Node) advanceIndex() {
	atomic.AddUint64(&n.self.Index, 1)
}

func (n *Node) advanceTimestamp() {
	newTimestamp := time.Now().UnixNano()
	for {
		currentTimestamp := atomic.LoadInt64(&n.self.Timestamp)
		if atomic.CompareAndSwapInt64(&n.self.Timestamp, currentTimestamp, newTimestamp) {
			break
		}
	}
}

func (n *Node) runHeartbeatLoop() {
	for {
		time.Sleep(heartbeatTime)
		n.runHeartbeatActions()
	}
}

func (n *Node) runHeartbeatActions() {
	n.advanceTimestamp()
	n.runGossipDisseminator()
	n.runFailureDetector()
}

func (n *Node) runFailureDetector() {
	timestamp := atomic.LoadInt64(&n.self.Timestamp)
	n.members.UpdateTransaction(func(members *[]*Member) {
		n.markAndSweepFailedMembers(members, timestamp)
	})
}

func (n *Node) markAndSweepFailedMembers(members *[]*Member, timestamp int64) {
	eraseStart := 0
	for _, member := range *members {
		n.markIfFailed(member, timestamp)
		n.removeIfHardFailed(members, member, timestamp, &eraseStart)
	}
	n.eraseRemoved(members, eraseStart)
}

func (n *Node) markIfFailed(member *Member, timestamp int64) {
	if !member.Stale && member.hasFailed(timestamp) {
		member.Stale = true
		log.Println("Member", member.Name, "went stale", member.Timestamp, "<", timestamp)
	}
}

func (m Member) hasFailed(timestamp int64) bool {
	return timestamp-m.Timestamp >= memberFailTimeout.Nanoseconds()
}

func (n *Node) removeIfHardFailed(members *[]*Member, member *Member, timestamp int64, eraseStart *int) {
	if !member.hasReachedHardLimit(timestamp) {
		(*members)[*eraseStart] = member
		*eraseStart++
	} else {
		log.Println("Member", member.Name, "reached hard stale limit, removing")
	}
}

func (m Member) hasReachedHardLimit(timestamp int64) bool {
	return timestamp-m.Timestamp >= memberRemoveTimeout.Nanoseconds()
}

func (n *Node) eraseRemoved(members *[]*Member, eraseStart int) {
	if eraseStart < len(*members) {
		*members = (*members)[:eraseStart]
	}
}

func (n *Node) runGossipDisseminator() {
	var updateMessage *gossip.MembersUpdate
	var gossipPeers []*Member

	n.members.ReadTransaction(func(members []*Member) {
		updateMessage = n.makeMembersUpdateMessage(members)
		gossipPeers = n.pickGossipGroup(members)
	})

	sb := strings.Builder{}
	for _, peer := range gossipPeers {
		sb.WriteString(peer.Name)
		sb.WriteString(", ")
	}
	log.Print("Sending gossip to", sb.String())

	n.sendGossips(gossipPeers, updateMessage)
}

func (n *Node) pickGossipGroup(members []*Member) []*Member {
	membersCount := len(members)
	if membersCount == 0 {
		return []*Member{}
	}

	indices := rand.Perm(membersCount)

	x := math.Log2(float64(membersCount))
	gossipRange := int(x)

	if gossipRange < membersCount {
		gossipRange++
	}

	gossipPeers := make([]*Member, 0, gossipRange)
	for _, peerIndex := range indices[:gossipRange] {

		// Should not happen, but just in case.
		if members[peerIndex].Name == n.self.Name {
			continue
		}

		memberCopy := new(Member)
		*memberCopy = *members[peerIndex]
		gossipPeers = append(gossipPeers, memberCopy)
	}

	return gossipPeers
}

func (n *Node) makeMembersUpdateMessage(members []*Member) *gossip.MembersUpdate {
	update := &gossip.MembersUpdate{
		Name:    n.self.Name,
		Members: getNonStaleMembersForGossip(members),
	}
	return update
}

func (n *Node) sendGossips(gossipPeers []*Member, updateMessage *gossip.MembersUpdate) {
	for _, peer := range gossipPeers {
		remote := peer.Address()
		n.sendMembersUpdate(remote, updateMessage)
	}
}

func (n *Node) sendMembersUpdate(remote string, update *gossip.MembersUpdate) {
	n.connectAndDo(remote, func(remoteNode gossip.GossipClient) {
		remoteNode.PushMembersUpdate(context.Background(), update)
	})
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

func (n *Node) handlePushHeartbeat(ctx context.Context, heartbeat *gossip.Heartbeat) (*empty.Empty, error) {
	var txErr error

	n.members.UpdateTransaction(func(members *[]*Member) {
		_, member := findMember(*members, mapToMember(heartbeat.Originator))
		if member == nil {
			txErr = errors.New("disconnected from cluster")
		} else {
			member.Timestamp = heartbeat.Originator.Timestamp
			member.Stale = false
		}
	})

	return &empty.Empty{}, txErr
}

func (n *Node) handlePushMembersUpdate(ctx context.Context, update *gossip.MembersUpdate) (*empty.Empty, error) {
	n.updateMembers(update.GetMembers())
	return &empty.Empty{}, nil
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
		key := MemberKey{Name: member.Name}
		membersMap[key] = member
	}

	for _, gmember := range gmembers {
		key := MemberKey{Name: gmember.Name}
		if member, ok := membersMap[key]; ok {
			n.updateMember(member, gmember)
		} else if gmember.Name != n.self.Name {
			n.addMember(members, mapToMember(gmember))
		}
	}
}

func (n *Node) updateMember(member *Member, gmember *gossip.Member) {
	if member.Timestamp <= gmember.Timestamp {
		member.Timestamp = gmember.Timestamp
		member.Index = gmember.Index
		member.Host = gmember.Host
		member.Port = uint16(gmember.Port)
		member.Stale = false
	}
}

func (n *Node) addMember(members *[]*Member, newMember *Member) {
	log.Println("Add", newMember.Name, "to cluster")
	*members = append(*members, newMember)
}

func (n *Node) RequestJoin(remote string) error {

	log.Println("Attempting to join cluster, connecting ", remote)

	var connectErr error

	n.connectAndDo(remote, func(remoteNode gossip.GossipClient) {
		remoteHost, _ := splitAddr(remote)
		request := &gossip.JoinRequest{
			Origiator:       mapToGossipMember(n.self),
			DestinationHost: remoteHost,
		}
		response, joinErr := remoteNode.Join(context.Background(), request)
		if joinErr != nil {
			log.Printf("Join error: %s\n", joinErr.Error())
			connectErr = joinErr
			return
		}
		logRequest(response)
		if response.Members == nil || len(response.Members) == 0 {
			log.Println("Join returned empty members list")
			connectErr = errors.New("Join returned empty members list")
			return
		}

		log.Println("Discovered self host: ", response.OriginatorHost)
		n.self.Host = response.OriginatorHost

		n.updateMembers(response.GetMembers())

		log.Println("Connected", remote, "and successfully joined cluster")
	})

	return connectErr
}

func (n *Node) handleJoin(ctx context.Context, request *gossip.JoinRequest) (*gossip.JoinResponse, error) {
	newMember, err := n.makeNewMember(ctx, request)
	if err != nil {
		return nil, err
	}

	response := &gossip.JoinResponse{
		OriginatorHost: newMember.Host,
	}

	// TODO: possible race and lack of memory barrier, fix it
	if n.self.Host == "" {
		n.self.Host = request.DestinationHost
	}

	n.members.UpdateTransaction(func(members *[]*Member) {
		response.Members = getNonStaleMembersForGossip(*members)
		n.addMember(members, newMember)
	})

	return response, nil
}

func getNonStaleMembersForGossip(members []*Member) []*gossip.Member {
	return filterAndMapToGossipMembers(members, func(member *Member) bool {
		return !member.Stale
	})
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
		if member.Name == wanted.Name {
			return index, member
		}
	}
	return -1, nil
}

func splitAddr(remote string) (string, int) {
	for idx := len(remote) - 1; idx >= 0; idx-- {
		if remote[idx] == ':' {
			port, err := strconv.Atoi(remote[idx+1:])
			if err != nil {
				break
			}
			return remote[:idx], port
		}
	}
	return "", -1
}
