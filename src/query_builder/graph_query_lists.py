from .grammar_to_query import *

RDF_GRAPHS = [#"core", "pathways", "go_hierarchy", "enzyme", "eclass", 
"taxonomy"]#, "geospecies", "go"]
MEMORY_ALIAS_GRAPHS = ["apache", "init", "mm", "ipc", "lib", "block", "arch",
                        "crypto", "security", "sound", "net", "fs", "drivers", "postgre", "kernel"]
JAVA_GRAPHS = ["avrora", "batik", "eclipse", "fop", "h2", "jython", "luindex", "lusearch", "pmd",
                "sunflow", "tomcat", "tradebeans", "tradesoap", "xalan"]

RDF_QUERIES = [g1, g2, geo]
MEMORY_ALIAS_QUERIES = [memoryalias]
JAVA_QUERIES = ["java"]

RDF_DATA = (RDF_GRAPHS, RDF_QUERIES)
MEMORY_ALIAS_DATA = (MEMORY_ALIAS_GRAPHS, MEMORY_ALIAS_QUERIES)
JAVA_DATA = (JAVA_GRAPHS, JAVA_QUERIES)
