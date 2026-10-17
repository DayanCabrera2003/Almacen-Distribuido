# Deploying a cluster by hand

Every node in this system is the same kind of node: there is no coordinator,
no metadata server and no client process. That is why there is a single
`docker/Dockerfile`, and why deploying a cluster is the same `docker run`
repeated once per node with a different identity.

The cluster is deployed with plain `docker` commands rather than an
orchestration file, so that each decision — the network, the container names,
how nodes find each other, where data lives — is a command you type and can
see. This guide is the step-by-step for a five-node cluster.

## 1. Build the image

From the repository root:

    docker build -f docker/Dockerfile -t almacen:latest .

The build context is the repository root because the image needs
`pyproject.toml` and `almacen/`; the Dockerfile itself lives under `docker/`.

## 2. Create the network

    docker network create almacen-net

A user-defined bridge network, not the default `bridge`: only on a
user-defined network does Docker run an embedded DNS server that resolves a
container's name to its address. The nodes rely on that — `ALMACEN_PEERS`
names peers by hostname (`node2:50051`), and the hostname is the container
name given with `--name` below.

## 3. Create one volume per node

    for i in 1 2 3 4 5; do docker volume create almacen-node$i-data; done

Each node keeps its blobs and its SQLite metadata under `/data`. A named volume
per node makes that state survive a container being removed and recreated —
which is what lets you stop a node and bring it back with its data, as a real
crashed-and-restarted machine would.

## 4. Start the nodes

Every node gets the same peer list, which **includes itself**: a node finds its
own entry by `ALMACEN_NODE_ID`, and placement (rendezvous hashing) must rank
the same set of nodes everywhere or nodes would disagree on where a blob lives.

    PEERS="node1@node1:50051,node2@node2:50051,node3@node3:50051,node4@node4:50051,node5@node5:50051"

    for i in 1 2 3 4 5; do
      docker run -d \
        --name node$i \
        --hostname node$i \
        --network almacen-net \
        -p 800$i:8000 \
        -v almacen-node$i-data:/data \
        -e ALMACEN_NODE_ID=node$i \
        -e ALMACEN_PEERS="$PEERS" \
        -e ALMACEN_GRPC_PORT=50051 \
        -e ALMACEN_REPLICATION_FACTOR=3 \
        -e ALMACEN_WRITE_QUORUM=2 \
        -e ALMACEN_GOSSIP_INTERVAL_SECONDS=2 \
        -e ALMACEN_SUSPICION_TIMEOUT_SECONDS=6 \
        almacen:latest
    done

What each flag decides:

| Flag | Why |
| --- | --- |
| `--name node$i` | The container's DNS name on `almacen-net`; must match the host part of its entry in `ALMACEN_PEERS` |
| `--network almacen-net` | Puts the node where its peers can resolve and reach it |
| `-p 800$i:8000` | Publishes the HTTP API on the host. The gRPC port (50051) is **not** published: only nodes talk gRPC, and they do it inside the network |
| `-v almacen-node$i-data:/data` | The node's durable state |
| `-e ALMACEN_NODE_ID` | Which entry of the peer list is "me" |
| `-e ALMACEN_GOSSIP_INTERVAL_SECONDS=2`, `..._SUSPICION_TIMEOUT_SECONDS=6` | Shortened from the production defaults (5s / 15s) so reconciliation and failure detection are observable within a demo session |

The full list of variables is in the README's Configuration section.

Nodes have no startup ordering, so a request in the first seconds may return
503 while peers are still binding their gRPC ports. Give the cluster a moment.

## 5. Use it

Nodes answer on ports 8001-8005. Any node accepts any request:

    curl -F "file=@README.md" -F "name=README.md" -F "tags=demo" http://127.0.0.1:8001/files
    curl "http://127.0.0.1:8005/files?tags=demo"

Inspect a node with `docker logs node3`, or open its API docs at
http://127.0.0.1:8003/docs.

## 6. Inject a partition

Disconnecting a container from the network isolates it from every peer:

    docker network disconnect almacen-net node5
    # ... write to other nodes; node5 cannot see them ...
    docker network connect almacen-net node5

The README walks through the full partition-and-heal demo.

## 7. Stop a node, bring it back

    docker stop node3        # crash: the others suspect, then declare it dead
    docker start node3       # recovery: same container, same volume, same data

## 8. Tear down

    docker rm -f node1 node2 node3 node4 node5
    docker network rm almacen-net
    docker volume rm almacen-node1-data almacen-node2-data almacen-node3-data \
      almacen-node4-data almacen-node5-data

Removing the volumes erases all stored data. Leave them in place to restart the
cluster later with its state.
