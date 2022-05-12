import csv
from tqdm import tqdm
import os
from src import RDF_DATA, MEMORY_ALIAS_DATA, JAVA_DATA, single_source, multiple_source, rdf_load
import cfpq_data
import redis
from redis.commands.graph import Graph
import subprocess
#from redisgraph-bulk-loader import bulk_insert

PATH_TO_GRAPHS="/home/jblab/projects/Timur/LoadGraphCSV"

MULTIPLE_SOURCE_CHUNKS = [10, 20, 50, 100]
MULT_SOURCE_QUERY_AMOUNT = 30
QUERY_TIMEOUT = 1200000
CACHE_SIZE = '120000'
PATH_TO_REDISGRAPH = '/home/jblab/projects/Timur/RedisGraph-anypair/RedisGraph/src/redisgraph.so'
PATH_TO_REDIS = '/home/jblab/projects/Timur/redis/src/redis-server'
#PATH_TO_GRAPHS = '/home/jblab/.local/lib/python3.8/site-packages/cfpq_data/data'

def run_benchmark_single_source():
    queries_rdf = ["g1", "g2", "geo"]
    query_pt = ["pointsTo"]
    for graph in RDF_DATA[0]:# + MEMORY_ALIAS_DATA[0]:
        with open(os.getcwd() + '/data/Chunk_1/{}.txt'.format(graph)) as f:
            lines = [line[:-1] for line in f.readlines()]
        path = f"{PATH_TO_GRAPHS}/{graph}/{graph}" 
        
        for query, query_name in (zip(RDF_DATA[1], queries_rdf) if graph in RDF_DATA[0] else zip(MEMORY_ALIAS_DATA[1], query_pt)):
            subprocess.run([PATH_TO_REDIS, "--daemonize", "yes", "--loadmodule", PATH_TO_REDISGRAPH, "CACHE_SIZE", CACHE_SIZE])
            pool = redis.ConnectionPool(host='localhost', port=6379)
            redis_con = redis.Redis(connection_pool=pool)
            print(f"Running {query_name} on {graph}.")
            subprocess.run(["redisgraph-bulk-insert", graph, "-n", path+"_nodes.csv", 
                        "-R", "type", path+"_type.csv", "-R", "subClassOf", path+"_subClassOf.csv", "-R", "other", 
                        path+"_other.csv"])
            redis_graph = redis_con.graph(graph)
            #redis_graph = rdf_load(redis_con, path, graph)
            
            with open(os.getcwd() + '/result/batch_16/single_source/{}_{}.csv'.format(graph, query_name), 'w') as file:
                writer = csv.writer(file, delimiter=' ', quoting=csv.QUOTE_MINIMAL)

                for source in tqdm(lines):
                    res = redis_graph.query(query + single_source(source), read_only=True, timeout=QUERY_TIMEOUT)
                    writer.writerow([res.result_set, res.run_time_ms])
                    redis_con.script_flush()
            #redis_graph.delete()
            #redis_con.flushall()
            print(f'Memory consumed: {redis_con.memory_stats()["peak.allocated"]}')
            #redis_con.shutdown(nosave=True)
        
        lines.clear()
        
def run_benchmark_multiple_source():
    queries_rdf = ["g1", "g2", "geo"]
    query_pt = ["pointsTo"]
    for graph in RDF_DATA[0]:# + MEMORY_ALIAS_DATA[0]:
        path = f"{PATH_TO_GRAPHS}/{graph}/{graph}.csv" 
        
        #subprocess.run([PATH_TO_REDIS, "--daemonize", "yes", "--loadmodule", PATH_TO_REDISGRAPH, "CACHE_SIZE", CACHE_SIZE])
        pool = redis.ConnectionPool(host='localhost', port=6379)
        redis_con = redis.Redis(connection_pool=pool)

        #redis_graph = rdf_load(redis_con, path, graph)

        for chunk_size in MULTIPLE_SOURCE_CHUNKS:
            with open(os.getcwd() + '/data/Chunk_{}/{}.txt'.format(chunk_size, graph)) as f:
                lines = [line[:-1] for line in f.readlines()]

            for query, query_name in (zip(RDF_DATA[1], queries_rdf) if graph in RDF_DATA[0] else zip(MEMORY_ALIAS_DATA[1], query_pt)):
                print(f"Running {query_name} on {graph}, chunk size {chunk_size}.")
                
                with open(os.getcwd() + '/result/batch_16/Chunk_{}/{}_{}.csv'.format(chunk_size, graph, query_name), 'w') as file:
                    writer = csv.writer(file, delimiter=' ', quoting=csv.QUOTE_MINIMAL)

                    for runs in tqdm(range(MULT_SOURCE_QUERY_AMOUNT)):
                        res = redis_con.query()
                        #res = redis_graph.query(query + multiple_source(lines), read_only=True, timeout=QUERY_TIMEOUT)
                        writer.writerow([res.result_set, res.run_time_ms])
                        redis_con.script_flush()
        redis_graph.delete()
        redis_con.flushall()
        print(f'Memory consumed: {redis_con.memory_stats()["peak.allocated"]}')
        redis_con.shutdown(nosave=True)
    
        lines.clear()

run_benchmark_single_source()
#run_benchmark_multiple_source()
