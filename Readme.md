# chord.py
A toy Python implementation of Chord DHT.

## Examples:
These examples show the HTTP implementation but you can also use the XMLRPC
implementation by substituting `chord.http.*` with `chord.xmlrpc.*` in each
example.

Start server:
```
$ python -m chord.http.server 127.0.0.1 4567 7
INFO:__main__:Running on 127.0.0.1:4567 with ring size 7...
```

Create node:
```
$ python -m chord.http.client 127.0.0.1 4567 --create
INFO:__main__:Creating node ring at [chord.node(127.0.0.1:4567)]
INFO:__main__:None
```

Join ring:
```
$ python -m chord.http.client 127.0.0.1 4569 --join 127.0.0.1:4567
INFO:__main__:Joining [chord.node(127.0.0.1:4569)] to node [chord.node(127.0.0.1:4567)]
INFO:__main__:None
```

Find successor:
```
$ python -m chord.http.client 127.0.0.1 4568 --find_successor 0
INFO:__main__:Finding successor for [0] starting at [chord.node(127.0.0.1:4568)]
INFO:__main__:(chord.node(127.0.0.1:4568), 3)
```

Put value:
```
$ python -m chord.http.client 127.0.0.1 4568 --put foo=bar
INFO:__main__:Putting key [foo] = value [bar]
INFO:__main__:{'hops': 2, 'storage_node': '127.0.0.1:4567'}
```

Get value:
```
$ python -m chord.http.client 127.0.0.1 4568 --get foo
INFO:__main__:Getting key [foo]
INFO:__main__:{'hops': 2, 'storage_node': '127.0.0.1:4567', 'value': 'bar'}
```

Shutdown node gracefully: (ctrl+c for ungraceful shutdown)
```
$ python -m chord.http.client 127.0.0.1 4568 --shutdown
```

## TODOs
* Dockerize application
* Figure out configuration

## Resources
* [Chord: A Scalable Peer-to-peer Lookup Protocol for Internet Applications](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf)
* [How to Make Chord Correct](https://arxiv.org/pdf/1502.06461.pdf)
