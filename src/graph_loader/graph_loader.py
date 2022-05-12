import hashlib

from redis import Redis
from redis.commands.graph import Node as RedisNode, Edge as RedisEdge
#from redisgraph import Node as RedisNode, Edge as RedisEdge
from tqdm import tqdm

from .graph import Graph as RedisGraph
from .triplet_loader import load_rdf_graph

BLOCK_SIZE = 100
IMPORTANT_EDGE_LABELS = ["subClassOf", "type", "broaderTransitive", "A", "D"]

def make_node(value: str, alias=None):
    return RedisNode(
        label='Node', alias=alias, properties={
            'value': int(value.replace('"', ''))
        })


def load_in_redis(rdf_graph, redis_graph: RedisGraph):
    all_nodes = dict()

    # Add all nodes from file to hash map
    for subj, obj, _ in rdf_graph:
        all_nodes.setdefault(subj, make_node(subj))
        all_nodes.setdefault(obj, make_node(obj))

    #print('Add all nodes to redis graph')
    for k, v in tqdm(all_nodes.items()):
        redis_graph.add_node(v)
        if len(redis_graph.nodes) > BLOCK_SIZE:
            redis_graph.flush()
    redis_graph.flush()

    # Create index over Nodes
    redis_graph.query("CREATE INDEX ON :Node(value)")

    #print('Add edges to existing nodes')
    for subj, obj, pred in tqdm(rdf_graph):
        edge = RedisEdge(all_nodes[subj], (pred if pred in IMPORTANT_EDGE_LABELS else "other"), all_nodes[obj])
        redis_graph.add_node(all_nodes[subj])
        redis_graph.add_node(all_nodes[obj])
        redis_graph.add_edge(edge)
        if len(redis_graph.edges) > BLOCK_SIZE:
            redis_graph.flush_edges()
    redis_graph.flush_edges()


def rdf_load(redis_con: Redis, rdf_file: str, redis_graph_name: str, rdf_format='csv'):
    rdf_graph = load_rdf_graph(rdf_file, rdf_format)

    redis_graph = RedisGraph(redis_con, redis_graph_name)

    load_in_redis(rdf_graph, redis_graph)
    return redis_graph
