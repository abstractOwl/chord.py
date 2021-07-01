#!/bin/bash

#
# Script to bootstrap a 3-node Chord.py ring using Docker.
#

function teardown() {
    echo "Running cleanup..."
    docker container stop chordpy_node1 chordpy_node2 chordpy_node3 > /dev/null
    docker container rm chordpy_node1 chordpy_node2 chordpy_node3 > /dev/null
    docker network rm chordnet > /dev/null
}

# Tear down any remaining resources in case of unclean shutdown
teardown

# Re-build local image
docker image build -t chordpy .

# Create local network for nodes
docker network create --internal chordnet

# Run containers
docker container run --detach --hostname node1 --name chordpy_node1 --network chordnet chordpy
docker container run --detach --hostname node2 --name chordpy_node2 --network chordnet chordpy
docker container run --detach --hostname node3 --name chordpy_node3 --network chordnet chordpy

# Bootstrap cluster
docker container exec chordpy_node1 python -m chord.http.client node1 5000 --create
docker container exec chordpy_node2 python -m chord.http.client node2 5000 --join node1:5000
docker container exec chordpy_node3 python -m chord.http.client node3 5000 --join node1:5000

# Open an interactive terminal on node3
docker container exec -it chordpy_node3 /bin/bash

# Silently tear down resources
teardown
