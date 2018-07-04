FROM golang
MAINTAINER Jaroslaw Mroz

#
# Needs to be run in project root!
#

RUN mkdir -p /$GOPATH/src/github.com/j-mroz/gossip-cluster
ADD . /$GOPATH/src/github.com/j-mroz/gossip-cluster

RUN cd /$GOPATH/src/github.com/j-mroz/gossip-cluster && \
    go build -o /root/gossip-node

EXPOSE 7913

CMD ["/root/gossip-node", "-host=:7913"]
