import os
import shutil
import argparse
from pathlib import Path
import cfpq_data
import numpy as np


def generate_nodes(path_to_dir, file_name, node_number):
    path_to_file = path_to_dir + "/" + file_name + "_nodes.csv"
    nodes = np.random.permutation(node_number)
    with open(path_to_file, 'w') as f:
        f.write("value\n")
        for node in nodes:
            f.write(f"{node}\n")


def graph_to_csv(graph_name, path_to_dir, graph_path):

    if os.path.exists(path_to_dir):
        print(f"REMOVE {path_to_dir}")
        shutil.rmtree(path_to_dir)

    path_to_dir = Path(path_to_dir).resolve()

    path_to_dir.mkdir(parents=True, exist_ok=True)

    csv_files = dict()
    for relationship in relationships + ["other"]:
        csv_file = path_to_dir / f"{graph_name}_{relationship}.csv"
        csv_files[relationship] = csv_file

        with open(csv_file, "w") as f:
            f.write("from,to\n")

    with open(graph_path, "r") as fin:
        for line in fin:
            u, v, label = line.strip().split()

            if label in relationships:
                with open(csv_files[label], "a") as fout:
                    fout.write(f"{u},{v}\n")
            else:
                with open(csv_files["other"], "a") as fout:
                    fout.write(f"{u},{v}\n")

    return csv_files


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='command line interface for graph downloader')
    parser.add_argument(
        '--graph'
        , required=True
        , type=str
        , help='graph name'
    )
    #parser.add_argument(
    #    '--relationships'
    #    , required=True
    #    , type=str
    #    , help='comma separated list of relationships'
    #)
    args = parser.parse_args()

    graph_name = args.graph
    relationships = args.relationships.split(',')

    path = Path(".").parent.resolve()
    path_to_graph = cfpq_data.download(graph_name)
    graph = cfpq_data.graph_from_csv(path_to_graph)
    graph_to_csv(graph_name, f"{path}/" + graph_name, path_to_graph)
    generate_nodes(f"{path}/" + graph_name, graph_name, graph.number_of_nodes())
