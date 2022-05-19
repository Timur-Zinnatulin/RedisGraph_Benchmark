g1 = "PATH PATTERN S = ()-/ [ <:subClassOf [ () | ~S ] :subClassOf ] | [ <:type [ () | ~S ] :type ]/->() "
g2 = "PATH PATTERN S = ()-/ <:subClassOf [ () | ~S ] :subClassOf /->() "
geo = "PATH PATTERN S = ()-/ :broaderTransitive [ () | ~S ] <:broaderTransitive /->() "

memoryalias = "PATH PATTERN V1 = ()-/ [~V2 <:A ~V1] | () /->() PATH PATTERN V2 = ()-/ ~S | () /->() PATH PATTERN V3 = ()-/ [:A ~V2 ~V3] | ()/->() PATH PATTERN S = ()-/ <:D ~V1 ~V2 ~V3 :D /->() "

all_pairs = "MATCH ()-/ ~S /->() RETURN COUNT (*)"

def single_source(id: int): 
    return f"MATCH (a)-/ ~S /->() WHERE a.value = {id} RETURN COUNT (*)"

def multiple_source(chunk: list):
    return f"MATCH (a)-/ ~S /->() WHERE a.value IN {chunk} RETURN COUNT(*)"
