# chord.py
A toy Python implementation of Chord DHT.

## Examples:
These examples show the HTTP implementation but you can also use the XMLRPC
implementation by substituting `chord.http.*` with `chord.xmlrpc.*` in each
example.

Start server:
```
$ python -m chord.http.server 127.0.0.1 4567 7
INFO:server:Running on 127.0.0.1:4567 with ring size 7...
```

Create node:
```
$ python -m chord.http.client 127.0.0.1 4567 create
INFO:__main__:Creating node ring at [chord.node(127.0.0.1:4567)]
INFO:__main__:CreateResponse()
```

Join ring:
```
$ python -m chord.http.client 127.0.0.1 4569 join 127.0.0.1 4567
INFO:__main__:Joining [chord.node(127.0.0.1:4569)] to node [chord.node(127.0.0.1:4567)]
INFO:__main__:JoinResponse()
```

Find successor:
```
$ python -m chord.http.client 127.0.0.1 4568 find_successor 0
INFO:__main__:Finding successor for [0] starting at [chord.node(127.0.0.1:4568)]
INFO:__main__:FindSuccessorResponse(node=chord.node(127.0.0.1:4568), hops=3)
```

Put value:
```
$ python -m chord.http.client 127.0.0.1 4568 put foo bar
INFO:__main__:Putting key [foo] = value [bar]
INFO:__main__:PutKeyResponse(storage_node=chord.node(127.0.0.1:4567), hops=2)
```

Get value:
```
$ python -m chord.http.client 127.0.0.1 4568 get foo
INFO:__main__:Getting key [foo]
INFO:__main__:GetKeyResponse(storage_node=chord.node(127.0.0.1:4567), hops=2, value='bar', found=True)
```

Shutdown node gracefully: (ctrl+c for ungraceful shutdown)
```
$ python -m chord.http.client 127.0.0.1 4568 shutdown
```

## Using Docker
A Docker script is included that bootstraps a 3-node Chord cluster and drops
into an interactive terminal. Nodes use the hostnames node1, node2, node3.

```
$ bash create_cluster.sh

Step 1/6 : FROM python:latest
 ---> 9b0d330dfd02
Step 2/6 : COPY requirements.txt requirements.txt
 ---> Using cache
 ---> 1af447c35778
Step 3/6 : RUN pip3 install -r requirements.txt
 ---> Using cache
 ---> ee1585194f65
Step 4/6 : COPY . /src
 ---> 418a6a75764c
Step 5/6 : WORKDIR /src
 ---> Running in a2d9e5d0f6ea
Removing intermediate container a2d9e5d0f6ea
 ---> 6a2fdb40de02
Step 6/6 : ENTRYPOINT ["bin/run_chord.sh"]
 ---> Running in 7c6a14386a6b
Removing intermediate container 7c6a14386a6b
 ---> 63f62f6a59fd
Successfully built 63f62f6a59fd
Successfully tagged chordpy:latest
b8615cda67a1de0818d338547c7abbd0114fd8ddf3b08d44cd771394a9d0efad
b4722111be819606dd0f31a82cf40cd562314c9b486cb78d6d76426b9406b60f
9a9397302ec9c2112154f3c9a023488bc41c25f89e55882b6341eaccce973b98
ddb24be3a141dd54ea68159f75296533bd39c64df8e98d396e541f30f11f0fa5
INFO:__main__:Creating node ring at [chord.node(node1:5000)]
INFO:__main__:CreateResponse()
INFO:__main__:Joining [chord.node(node2:5000)] to node [chord.node(node1:5000)]
INFO:__main__:JoinResponse()
INFO:__main__:Joining [chord.node(node3:5000)] to node [chord.node(node1:5000)]
INFO:__main__:JoinResponse()

root@node3:/src# python3 -m chord.http.client node3 5000 put foo bar
INFO:__main__:Putting key [foo] = value [bar]
INFO:__main__:PutKeyResponse(storage_node=chord.node(node1:5000), hops=2)
root@node3:/src# python3 -m chord.http.client node3 5000 --get foo
INFO:__main__:Getting key [foo]
INFO:__main__:GetKeyResponse(storage_node=chord.node(node1:5000), hops=2, value='bar', found=True)
```

## Resources
* [Chord: A Scalable Peer-to-peer Lookup Protocol for Internet Applications](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf)
* [How to Make Chord Correct](https://arxiv.org/pdf/1502.06461.pdf)
