# chord.py
A toy Python implementation of Chord DHT:
https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf

## Examples:
Start server:
```
> python -m chord.server 127.0.0.1 4568 7
```

Create node:
```
> python -m chord.client 127.0.0.1 4567 7 --create
```

Join ring:
```
python -m chord.client 127.0.0.1 4569 7 --join 127.0.0.1:4567
```

Find successor:
```
python -m chord.client 127.0.0.1 4568 7 --find_successor 0
```

## TODOs
* Implement storage
* Clean up dead successors
