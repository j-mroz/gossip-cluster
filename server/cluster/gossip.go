package cluster

import (
	"context"
	"encoding/json"
	"log"
	"reflect"
	"strings"

	"github.com/golang/protobuf/ptypes/empty"
	gossip "github.com/j-mroz/gossip-cluster/proto/gossip/v1"
)

// Implements gossip.v1 proto.
type GossipServer struct {
	node *Node
}

// ListMembers returns members that GossipServer known.
func (s GossipServer) PullMembers(ctx context.Context, req *gossip.PullMembersRequest) (*gossip.PullMembersResponse, error) {
	logRequest(req)
	return s.node.handlePullMembers(ctx, req)
}

func (s GossipServer) PushHeartbeat(ctx context.Context, heartbeat *gossip.Heartbeat) (*empty.Empty, error) {
	logRequest(heartbeat)
	return s.node.handlePushHeartbeat(ctx, heartbeat)
}

func (s GossipServer) PushMembersUpdate(ctx context.Context, update *gossip.MembersUpdate) (*empty.Empty, error) {
	logRequest(update)
	return s.node.handlePushMembersUpdate(ctx, update)
}

// Join handles Join requests from other GossipServers.
func (s GossipServer) Join(ctx context.Context, req *gossip.JoinRequest) (*gossip.JoinResponse, error) {
	logRequest(req)
	return s.node.handleJoin(ctx, req)
}

func logRequest(req interface{}) {
	bytes, _ := json.MarshalIndent(req, ">", "  ")
	//bytes, _ := json.Marshal(req)

	typeInfo := reflect.TypeOf(req)
	typeNameParts := strings.Split(typeInfo.String(), ".")
	msgType := typeNameParts[len(typeNameParts)-1]
	log.Println("Recv:", msgType, string(bytes))
}
