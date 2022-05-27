import csv
from tqdm import tqdm
from pathlib import Path
import os
from src import RDF_DATA, MEMORY_ALIAS_DATA
from src import all_pairs, single_source, multiple_source
import redis
from redis.exceptions import ConnectionError as RedisError
from redis.commands.graph import Graph
import subprocess

PATH_TO_HERE = Path.cwd().resolve()

# Insert path to folder with graphs here
PATH_TO_GRAPHS = f"{PATH_TO_HERE}/data/Graphs"


# Insert path to redis here
PATH_TO_REDIS = "/home/jblab/projects/Timur/redis/"

MULTIPLE_SOURCE_CHUNKS = [10, 20, 50, 100]
MULT_SOURCE_QUERY_AMOUNT = 30
QUERY_TIMEOUT = 1200000


def load_graph(graph: str, path: str):
    if graph in RDF_DATA[0]:
        if graph in ['geospecies', 'enzyme']:
            subprocess.run([
                "redisgraph-bulk-insert", graph, "-n", path+f"/{graph}_nodes.csv",
                "-R", "type", path+f"/{graph}_type.csv",
                "-R", "subClassOf", path+f"/{graph}_subClassOf.csv",
                "-R", "broaderTransitive", path+f"/{graph}_broaderTransitive.csv",
                "-R", "other", path+f"/{graph}_other.csv"])
        else:
            subprocess.run([
                "redisgraph-bulk-insert", graph, "-n", path+f"/{graph}_nodes.csv",
                "-R", "type", path+f"/{graph}_type.csv",
                "-R", "subClassOf", path+f"/{graph}_subClassOf.csv",
                "-R", "other", path+f"/{graph}_other.csv"])
    else:
        subprocess.run([
            "redisgraph-bulk-insert", graph, "-n", path+f"/{graph}_nodes.csv",
            "-R", "A", path+f"/{graph}_A.csv",
            "-R", "subClassOf", path+f"/{graph}_D.csv",
            "-R", "other", path+f"/{graph}_other.csv"])

def run_benchmark_all_pairs():
    queries_rdf = ["g1", "g2", "geo"]
    query_pt = ["pointsTo"]
    for graph in RDF_DATA[0] + MEMORY_ALIAS_DATA[0]:
        path = f"{PATH_TO_GRAPHS}/{graph}"
        if not os.path.exists(path):
            continue
        subprocess.run([PATH_TO_REDIS + "src/redis-server", PATH_TO_REDIS + "redis.conf", "--daemonize", "yes"])
        pool = redis.ConnectionPool(host='localhost', port=6379)
        redis_con = redis.Redis(connection_pool=pool)
        redis_con.config_set("CACHE_SIZE", 100000)

        load_graph(graph, path)
        redis_graph = Graph(redis_con, graph)

        try:
            for query, query_name in (zip(RDF_DATA[1], queries_rdf) if graph in RDF_DATA[0] else zip(MEMORY_ALIAS_DATA[1], query_pt)):
                print(f"Running {query_name} on {graph}, all-pairs.")
                Path(f'{PATH_TO_HERE}/result/batch_16/all-pairs/{query_name}').mkdir(parents=True, exist_ok=True)
                with open(f'{PATH_TO_HERE}/result/batch_16/all-pairs/{query_name}/{graph}_{query_name}.csv', 'w') as file:
                    writer = csv.writer(file, delimiter=' ', quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(['answer', 'time'])
                    for runs in tqdm(range(MULT_SOURCE_QUERY_AMOUNT)):
                        res = redis_graph.query(query + all_pairs, read_only=True, timeout=QUERY_TIMEOUT)
                        writer.writerow([res.result_set, res.run_time_ms])
                        redis_con.script_flush()
        except RedisError:
            print(f"Redis server crashed on graph '{graph}', all-pairs, continuing...")
            continue

        redis_graph.delete()
        redis_con.flushall()
        redis_con.shutdown(nosave=True)

def run_benchmark_single_source():
    queries_rdf = ["g1", "g2", "geo"]
    query_pt = ["pointsTo"]
    for graph in RDF_DATA[0] + MEMORY_ALIAS_DATA[0]:
        path = f"{PATH_TO_GRAPHS}/{graph}"
        if not os.path.exists(path):
            continue

        with open(f'{PATH_TO_HERE}/data/Chunks/Chunk_1/{graph}.txt') as f:
            lines = [line[:-1] for line in f.readlines()]

        for query, query_name in (zip(RDF_DATA[1], queries_rdf) if graph in RDF_DATA[0] else zip(MEMORY_ALIAS_DATA[1], query_pt)):
            try:
                subprocess.run([PATH_TO_REDIS + "src/redis-server", PATH_TO_REDIS + "redis.conf", "--daemonize", "yes"])
                pool = redis.ConnectionPool(host='localhost', port=6379)
                redis_con = redis.Redis(connection_pool=pool)

                load_graph(graph, path)
                redis_graph = Graph(redis_con, graph)

                print(f"Running {query_name} on {graph}.")

                Path(f'{PATH_TO_HERE}/result/batch_16/Chunk_1/{query_name}').mkdir(parents=True, exist_ok=True)
                with open(f'{PATH_TO_HERE}/result/batch_16/Chunk_1/{query_name}/{graph}_{query_name}.csv', 'w') as file:
                    writer = csv.writer(file, delimiter=' ', quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(['answer', 'time'])

                    for source in tqdm(lines):
                        res = redis_graph.query(query + single_source(source), read_only=True, timeout=QUERY_TIMEOUT)
                        writer.writerow([res.result_set, res.run_time_ms])
                        redis_con.script_flush()
            except RedisError:
                print(f"Redis server crashed on graph '{graph}', single-source, continuing...")
                continue
            redis_graph.delete()
            redis_con.flushall()
            redis_con.shutdown(nosave=True)

        lines.clear()


def run_benchmark_multiple_source():
    queries_rdf = ["g1", "g2", "geo"]
    query_pt = ["pointsTo"]
    for graph in RDF_DATA[0] + MEMORY_ALIAS_DATA[0]:
        path = f"{PATH_TO_GRAPHS}/{graph}"
        if not os.path.exists(path):
            continue

        for chunk_size in MULTIPLE_SOURCE_CHUNKS:
            subprocess.run([PATH_TO_REDIS + "src/redis-server", PATH_TO_REDIS + "redis.conf", "--daemonize", "yes"])
            pool = redis.ConnectionPool(host='localhost', port=6379)
            redis_con = redis.Redis(connection_pool=pool)

            load_graph(graph, path)
            redis_graph = Graph(redis_con, graph)

            with open(f'{PATH_TO_HERE}/data/Chunks/Chunk_{chunk_size}/{graph}.txt') as f:
                lines = [line[:-1] for line in f.readlines()]
            try:
                for query, query_name in (zip(RDF_DATA[1], queries_rdf) if graph in RDF_DATA[0] else zip(MEMORY_ALIAS_DATA[1], query_pt)):
                    print(f"Running {query_name} on {graph}, chunk size {chunk_size}.")
                    Path(f'{PATH_TO_HERE}/result/batch_16/Chunk_{chunk_size}/{query_name}').mkdir(parents=True, exist_ok=True)
                    with open(f'{PATH_TO_HERE}/result/batch_16/Chunk_{chunk_size}/{query_name}/{graph}_{query_name}.csv', 'w') as file:
                        writer = csv.writer(file, delimiter=' ', quoting=csv.QUOTE_MINIMAL)
                        writer.writerow(['answer', 'time'])
                        for runs in tqdm(range(MULT_SOURCE_QUERY_AMOUNT)):
                            res = redis_graph.query(query + multiple_source(lines), read_only=True, timeout=QUERY_TIMEOUT)
                            writer.writerow([res.result_set, res.run_time_ms])
                            redis_con.script_flush()
            except RedisError:
                print(f"Redis server crashed on graph '{graph}', chunk size {chunk_size}, continuing...")
                continue

            redis_graph.delete()
            redis_con.flushall()
            redis_con.shutdown(nosave=True)

            lines.clear()
