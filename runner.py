import csv
from tqdm import tqdm
from pathlib import Path
from src import RDF_DATA, MEMORY_ALIAS_DATA, single_source, multiple_source
import redis
from redis.exceptions import ConnectionError as RedisError
from redis.commands.graph import Graph
import subprocess

#Insert path to folder with graphs here
PATH_TO_GRAPHS="/home/jblab/projects/Timur/LoadGraphCSV"

#Insert path to redis here
PATH_TO_REDIS = "/home/jblab/projects/Timur/redis/"

MULTIPLE_SOURCE_CHUNKS = [10, 20, 50, 100]
MULT_SOURCE_QUERY_AMOUNT = 30
QUERY_TIMEOUT = 1200000

def load_graph(graph: str, path: str):
    if graph in RDF_DATA[0]:
        if graph in ['geospecies', 'enzyme']:
            subprocess.run(["redisgraph-bulk-insert", graph, "-n", path+"_nodes.csv", 
                            "-R", "type", path+"_type.csv", 
                            "-R", "subClassOf", path+"_subClassOf.csv", 
                            "-R", "broaderTransitive", path+"_broaderTransitive.csv",
                            "-R", "other", path+"_other.csv"])
        else:
            subprocess.run(["redisgraph-bulk-insert", graph, "-n", path+"_nodes.csv", 
                            "-R", "type", path+"_type.csv", 
                            "-R", "subClassOf", path+"_subClassOf.csv", 
                            "-R", "other", path+"_other.csv"])
    else:
        subprocess.run(["redisgraph-bulk-insert", graph, "-n", path+"_nodes.csv", 
                        "-R", "A", path+"_A.csv", 
                        "-R", "subClassOf", path+"_D.csv", 
                        "-R", "other", path+"_other.csv"])


def run_benchmark_single_source():
    path_to_here = Path.cwd().resolve()
    queries_rdf = ["g1", "g2", "geo"]
    query_pt = ["pointsTo"]
    for graph in RDF_DATA[0] + MEMORY_ALIAS_DATA[0]:
        with open(f'{path_to_here}/data/Chunk_1/{graph}.txt') as f:
            lines = [line[:-1] for line in f.readlines()]
        path = f"{PATH_TO_GRAPHS}/{graph}/{graph}" 
        
        for query, query_name in (zip(RDF_DATA[1], queries_rdf) if graph in RDF_DATA[0] else zip(MEMORY_ALIAS_DATA[1], query_pt)):
            try:
                subprocess.run([PATH_TO_REDIS + "src/redis-server", PATH_TO_REDIS + "redis.conf", "--daemonize", "yes"])
                pool = redis.ConnectionPool(host='localhost', port=6379)
                redis_con = redis.Redis(connection_pool=pool)

                load_graph(graph, path)
                redis_graph = Graph(graph)
                
                print(f"Running {query_name} on {graph}.")

                Path(f'{path_to_here}/result/batch_16/Chunk_1/{query_name}').mkdir(parents=True, exist_ok=True)
                with open(f'{path_to_here}/result/batch_16/Chunk_1/{query_name}/{graph}_{query_name}.csv', 'w') as file:
                    writer = csv.writer(file, delimiter=' ', quoting=csv.QUOTE_MINIMAL)
                    writer.writerow(['answer', 'time'])
                    
                    for source in tqdm(lines):
                        res = redis_graph.query(query + single_source(source), read_only=True, timeout=QUERY_TIMEOUT)
                        writer.writerow([res.result_set, res.run_time_ms])
                        redis_con.script_flush()
            except RedisError:
                print(f"Redis server crashed on graph '{graph}', single-source, continuing...")
            redis_graph.delete()
            redis_con.flushall()
            redis_con.shutdown(nosave=True)
        
        lines.clear()
        
def run_benchmark_multiple_source():
    path_to_here = Path.cwd().resolve()
    queries_rdf = ["g1", "g2", "geo"]
    query_pt = ["pointsTo"]
    for graph in RDF_DATA[0] + MEMORY_ALIAS_DATA[0]:
        path = f"{PATH_TO_GRAPHS}/{graph}/{graph}.csv" 
        
        for chunk_size in MULTIPLE_SOURCE_CHUNKS:
            subprocess.run([PATH_TO_REDIS + "src/redis-server", PATH_TO_REDIS + "redis.conf", "--daemonize", "yes"])
            pool = redis.ConnectionPool(host='localhost', port=6379)
            redis_con = redis.Redis(connection_pool=pool)

            load_graph(graph, path)
            redis_graph = Graph(graph)

            with open(f'{path_to_here}/data/Chunk_{chunk_size}/{graph}.txt') as f:
                lines = [line[:-1] for line in f.readlines()]
            try:
                for query, query_name in (zip(RDF_DATA[1], queries_rdf) if graph in RDF_DATA[0] else zip(MEMORY_ALIAS_DATA[1], query_pt)):
                    print(f"Running {query_name} on {graph}, chunk size {chunk_size}.")
                    Path(f'{path_to_here}/result/batch_16/Chunk_{chunk_size}/{query_name}').mkdir(parents=True, exist_ok=True)
                    with open(f'{path_to_here}/result/batch_16/Chunk_{chunk_size}//{query_name}/{graph}_{query_name}.csv', 'w') as file:
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

run_benchmark_single_source()
run_benchmark_multiple_source()
