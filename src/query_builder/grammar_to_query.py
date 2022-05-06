g1 = "PATH PATTERN S = ()-/ [ <:subClassOf [ () | ~S ] :subClassOf ] | [ <:type [ () | ~S ] :type ]/->() "
g2 = "PATH PATTERN S = ()-/ <:subClassOf [ () | ~S ] :subClassOf /->() "
geo = "PATH PATTERN S = ()-/ :broaderTransitive [ () | ~S ] <:broaderTransitive /->() "
#MATCH ()-/ ~S /->() RETURN COUNT (*)

memoryalias = "PATH PATTERN V1 = ()-/ [~V2 <:A ~V1] | () /->() PATH PATTERN V2 = ()-/ ~S | () /->() PATH PATTERN V3 = ()-/ [:A ~V2 ~V3] | ()/->() PATH PATTERN S = ()-/ <:D ~V1 ~V2 ~V3 :D /->() "

all_pairs = "MATCH ()-/ ~S /->() RETURN COUNT (*)"

def single_source(id: int): 
    return f"MATCH (a)-/ ~S /->() WHERE a.value = '{id}' RETURN COUNT (*)"

#PATH PATTERN S = ()-/ :A :B* :C*/->() MATCH ()-/ ~S /->() RETURN COUNT(*)
#PATH PATTERN SA = ()-/ () | [:A ~SA] /->() PATH PATTERN SB = ()-/ () | [:B ~SB] /->() MATCH ()-/ ~SA ~SB /->() RETURN COUNT(*)

#cfpq_quijpers: 226669749 matches, 385.99 366.62 362.12 364.96 361.52 | 140.81 140.42 141.13

#redis-server --loadmodule /usr/lib/redis/modules/redisgraph.so &