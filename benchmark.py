from Coding.RedisGraph_Benchmark.src.query_runner.runner import run_benchmark_all_pairs, run_benchmark_multiple_source, run_benchmark_single_source
from src import query_runner
from argparse import ArgumentParser

"""
Select graph for benchmark
OR
Select set of graphs

Select query for benchmark (for given graph)

Select query scenario (all-pairs, ss, ms)
"""

def main():
    parser = ArgumentParser('Run benchmarks for RedisGraph')
    parser.add_argument('RDF_PATH', help='rdf graph path')
    parser.add_argument('--name', help='graph name', default=None)
    parser.add_argument('--host', help='redis host name', default='localhost')
    parser.add_argument('--port', help='redis port', default=6379)
    parser.add_argument('--scenario', help='CFPQ run scenario (all-pairs, single-source or multiple-source)', default=None)
    args = parser.parse_args()

    # graph_name = args.graph_name if args.name is not None else pathlib.Path(args.RDF_PATH).name

    if (args.scenario == 'all-pairs'):
        print("Running all-pairs scenario...")
        run_benchmark_all_pairs()
    elif (args.scenario == 'ss'):
        print("Running single-source scenario...")
        run_benchmark_single_source()
    elif (args.scenario == 'ms'):
        print("Running multiple-source scenario...")
        run_benchmark_multiple_source()
    elif (args.scenario is None):
        print("Please specify CFPQ scenario")


if __name__ == "__main__":
    main()