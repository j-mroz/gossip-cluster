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

// GossipServer implements gossip.v1 proto.
type GossipServer struct {
	node *Node
}

// PullMembers returns members that GossipServer known.
func (s GossipServer) PullMembers(ctx context.Context,
	req *gossip.PullMembersRequest) (*gossip.PullMembersResponse, error) {

	logRequest(req)
	return s.node.handlePullMembers(ctx, req)
}

// PushHeartbeat pushes the heartbeat to selected node
func (s GossipServer) PushHeartbeat(ctx context.Context,
	heartbeat *gossip.Heartbeat) (*empty.Empty, error) {

	logRequest(heartbeat)
	return s.node.handlePushHeartbeat(ctx, heartbeat)
}

// PushMembersUpdate pushes the update to selected node
func (s GossipServer) PushMembersUpdate(ctx context.Context,
	update *gossip.MembersUpdate) (*empty.Empty, error) {

	logRequest(update)
	return s.node.handlePushMembersUpdate(ctx, update)
}

// Join handles Join requests from other node.
func (s GossipServer) Join(ctx context.Context,
	req *gossip.JoinRequest) (*gossip.JoinResponse, error) {

	logRequest(req)
	return s.node.handleJoin(ctx, req)
}

func logRequest(req interface{}) {
	bytes, _ := json.Marshal(req)
	//bytes, _ := json.MarshalIndent(req, ">", "  ")
	typeInfo := reflect.TypeOf(req)
	typeNameParts := strings.Split(typeInfo.String(), ".")
	msgType := typeNameParts[len(typeNameParts)-1]
	log.Println("Recv:", msgType, string(bytes))
}
