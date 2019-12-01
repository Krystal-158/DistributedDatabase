"""
This file defines the graph used to detect deadlocks.
Each vertex in the graph represents a transaction.
If there's an edge pointing from T1 to T2, then T2 is 
in T1's adjacent list while T1 isn't in T2's.
"""

class Vertex:
    def __init__(self, vId):
        self.vId = vId
        self.adj = set()

    def addAdj(self, v):
        """
        Add a vertex into adjacent list
        """
        self.adj.add(v)

    def deleteAdj(self, v):
        """
        Remove a vertex from adjacent list
        """
        self.adj.discard(v)


class Graph:
    def __init__(self):
        self.vertices = list()
        self.edges = list()

    def insertVertex(self, vId):
        """
        Add a vertex if it's not in the graph
        """
        v = Vertex(vId)
        if v not in self.vertices:
            self.vertices.append(v)
    
    def getVertex(self, vId):
        """
        Find a particular vertex in the graph
        """
        for v in self.vertices:
            if v.vId == vId:
                return v
        return None

    def deleteVertex(self, vId):
        """
        Delete a vertex from the graph, thus delete all
        corresponding edges.
        """
        v = self.getVertex(vId)
        if v:
            self.vertices.remove(v)
            for u in self.vertices:
                u.deleteAdj(v)                

    def detectCycle(self):
        """
        Detect if there's a cycle in the graph.
        If so, return a list of vertices in the cycle
        Otherwise return an empty list
        """


    def tpsort(self):
        """
        Topological sort
        """
        