import pathlib

import rdflib


def load_triplets(filename: str):
    triplets = list()
    with open(filename, mode='r') as file:
        for line in file:
            try:
                subj, obj, pred = line[:-1].split()
                pred = pred.replace('-', '_')
                triplets.append((subj, obj, pred))
            except ValueError:
                continue
    return triplets


def uri_to_name(identifier, rdf_graph: rdflib.Graph):
    if isinstance(identifier, rdflib.URIRef):
        prefix, namespace, name = rdf_graph.compute_qname(identifier)
        return name
    return identifier


def load_rdf_graph(rdf_file: str, rdf_format=None):
    rdf_format = pathlib.Path(rdf_file).suffix[1:] if rdf_format is None else rdf_format
    if rdf_format == 'csv':
        return load_triplets(rdf_file)
    else:
        rdf_graph = rdflib.Graph()
        rdf_graph.load(rdf_file, format=rdf_format)
        return list(map(lambda spo: (spo[0], uri_to_name(spo[1], rdf_graph), spo[2]), iter(rdf_graph)))
