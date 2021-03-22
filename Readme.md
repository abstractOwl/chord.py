# chord.py
A toy Python implementation of Chord DHT.

## Examples:
Start server:
```
$ python -m chord.server 127.0.0.1 4568 17
```

Create node:
```
$ python -m chord.client 127.0.0.1 4567 --create
```

Join ring:
```
$ python -m chord.client 127.0.0.1 4569 --join 127.0.0.1:4567
```

Find successor:
```
$ python -m chord.client 127.0.0.1 4568 --find_successor 0
```

Put value:
```
$ python -m chord.client 127.0.0.1 4568 --put foo=bar
```

Get value:
```
$ python -m chord.client 127.0.0.1 4568 --get foo
```

Shutdown node gracefully: (ctrl+c for ungraceful shutdown)
```
$ python -m chord.client 127.0.0.1 4568 --shutdown
```

## TODOs
* Implement successor list
* Implement multiple buckets per node to increase likelihood of a more even
  distribution of keys

## Resources
* [Chord: A Scalable Peer-to-peer Lookup Protocol for Internet Applications](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf)
* [How to Make Chord Correct](https://arxiv.org/pdf/1502.06461.pdf)
