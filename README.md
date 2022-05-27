# RedisGraph_Benchmark

Benchmark platform for experimental study of context-free path queries on RedisGraph database.

## Requirements
+ [Redis](https://github.com/redis/redis) version >= 6.0.0
+ RedisGraph modification from the [YaccConstructor](https://github.com/YaccConstructor/RedisGraph) repository (don't forget --recurse-submodules)
+ [redisgraph-bulk-loader](https://github.com/RedisGraph/redisgraph-bulk-loader) for optimal graph loading

## How to use
+ Configure redis to load RedisGraph module on launch
+ Load graphs from the dataset
+ Launch benchmarks
+ Results are placed in the _results_ directory

## How to load graphs
```
$ python3 ./graph_loader.py -h
```
### Example
```
$ python3 ./graph_loader.py --graph rdf
```

# How to launch benchmarks
```
$ python3 ./benchmark.py -h
```

### Example
```
$ python3 ./benchmark.py --scenario all-pairs
```
