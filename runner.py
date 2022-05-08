import csv
from tqdm import tqdm
import os
from src import RDF_DATA, MEMORY_ALIAS_DATA, JAVA_DATA, single_source, all_pairs, rdf_load
import cfpq_data
from redisgraph import Graph

QUERY_TIMEOUT = 1200000

def run_benchmark():
    with open(os.getcwd() + '/data/single_source/geospecies.txt') as f:
        lines = [line[:-1] for line in f.readlines()]
    queries = ["g1", "g2", "geo"]
    for graph in RDF_DATA[0]:
        if (graph == "geospecies"):
            path = cfpq_data.download(graph)
            redis_graph = rdf_load(path, graph)
            for query, query_name in zip(RDF_DATA[1], queries):
                with open(os.getcwd() + '/result/single_source/{}_{}.csv'.format(graph, query_name), 'w') as file:
                    writer = csv.writer(file, delimiter=' ', quoting=csv.QUOTE_MINIMAL)
                    for source in tqdm(lines):
                        res = redis_graph.query(query + single_source(source), timeout=QUERY_TIMEOUT)
                        writer.writerow([res.result_set, res.run_time_ms])
            redis_graph.delete()

run_benchmark()