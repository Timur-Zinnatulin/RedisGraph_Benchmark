from redis.commands.graph import Graph as RedisGraph


class Graph(RedisGraph):
    def __init__(self, redis_con, name):
        super().__init__(redis_con, name)

    def commit_edges(self):
        """
        Create edges with existing nodes.
        """
        if len(self.nodes) == 0 and len(self.edges) == 0:
            return None

        query = 'MATCH '
        for _, node in self.nodes.items():
            query += str(node) + ','

        # Discard leading comma.
        if query[-1] == ',':
            query = query[:-1]

        query += ' CREATE '

        query += ','.join([str(edge) for edge in self.edges])

        # Discard leading comma.
        if query[-1] == ',':
            query = query[:-1]

        return self.query(query)

    def flush_edges(self):
        """
        Commit the edges and reset the edges and nodes to zero length
        """
        self.commit_edges()
        self.nodes = {}
        self.edges = []
