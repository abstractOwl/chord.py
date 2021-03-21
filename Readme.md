# chord.py
A toy Python implementation of Chord DHT:
https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf

## Examples:
Start server:
(Ring size should be sufficiently large to avoid node id collisions)
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

## TODOs
* Handle failing nodes
* Shift keys during clean node exit
