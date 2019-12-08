"""Xiaowen Yan
This file defines the graph used to detect deadlocks.
Each vertex in the graph represents a transaction.
If there's an edge pointing from T1 to T2, then T2 is 
in T1's adjacent list while T1 isn't in T2's.
"""
from queue import Queue
debugMode = True

class Vertex:
    def __init__(self, vId):
        """Xiaowen Yan
        DESCRIPTION: Create a vertex object
        INPUT: vId - vertex index
        No output and side effect
        """
        self.vId = vId
        self.visited = 0 # flag used in cycle detection
        self.adj = set() # adjacent list

    def addAdj(self, v):
        """Xiaowen Yan
        DESCRIPTION: Add Vertex v into current adjacent list
        INPUT: v - the vertex which will be added to self.adj
        No output and side effect
        """
        self.adj.add(v)

    def deleteAdj(self, v):
        """Xiaowen Yan
        DESCRIPTION: Remove a vertex from adjacent list
        INPUT: v - the vertex which will be deleted from adjacent list
        No output and side effect
        """
        self.adj.discard(v)


class Graph:
    def __init__(self):
        """Xiaowen Yan
        DESCRIPTION:
        INPUT:
        OUTPUT:
        SIDE EFFECTS:
        """
        self.vertices = list()
        self.edges = list()

    def insertVertex(self, vId):
        """Xiaowen Yan
        DESCRIPTION:
        INPUT:
        OUTPUT:
        SIDE EFFECTS:
        """
        """
        Add a vertex if it's not in the graph
        """
        v = Vertex(vId)
        if v not in self.vertices:
            self.vertices.append(v)
    
    def getVertex(self, vId):
        """Xiaowen Yan
        DESCRIPTION:
        INPUT:
        OUTPUT:
        SIDE EFFECTS:
        """
        """
        Find a particular vertex in the graph
        """
        for v in self.vertices:
            if v.vId == vId:
                return v
        return None

    def deleteVertex(self, vId):
        """Xiaowen Yan
        DESCRIPTION:
        INPUT:
        OUTPUT:
        SIDE EFFECTS:
        """
        """
        Delete a vertex from the graph, thus delete all
        corresponding edges.
        """
        v = self.getVertex(vId)
        if v:
            self.vertices.remove(v) # remove it from the graph
            # remove the vertex from all its neighbours' adjacent list
            for u in self.vertices:
                u.deleteAdj(v) 
            # delete the vertex
            del v               

    def addEdge(self, vId, uId):
        """Xiaowen Yan
        DESCRIPTION:
        INPUT:
        OUTPUT:
        SIDE EFFECTS:
        """
        """
        Add vertex uId to vertex vId's adjacent list
        uId is the current lock holder, vId is the waiting tx
        """
        v = self.getVertex(vId)
        u = self.getVertex(uId)
        v.addAdj(u)

    def deleteEdge(self, vId, uId):
        """Xiaowen Yan
        DESCRIPTION:
        INPUT:
        OUTPUT:
        SIDE EFFECTS:
        """
        """
        Delete vertex uId from vertex vId's adjacent list
        """
        v = self.getVertex(vId)
        u = self.getVertex(uId)
        v.deleteAdj(u)

    def detectCycle(self):
        """Xiaowen Yan
        DESCRIPTION:
        INPUT:
        OUTPUT:
        SIDE EFFECTS:
        """
        """
        Detect if there's a cycle in the graph.
        If so, return a list of vertices in the cycle
        Otherwise return an empty list
        """
        cycle = list() # list of all vertices in any possible cycles
        stack = list()
        for v in self.vertices:
            v.visited = 0
        for v in self.vertices:
            if v.visited == 0:
                self.dfs(v, stack, cycle)
        return cycle

    def dfs(self, v, stack, cycle):
        """Xiaowen Yan
        DESCRIPTION:
        INPUT:
        OUTPUT:
        SIDE EFFECTS:
        """
        """
        Depth-first search a vertex
        Return all vertices in a cycle
        """
        v.visited = 1
        stack.append(v)
        for u in v.adj:
            if u not in stack and u.visited == 0:
                self.dfs(u, stack, cycle)
            elif u in stack:
                index = stack.index(u)
                for w in stack[index:]:
                    if w not in cycle:
                        cycle.append(w)
        stack.pop(-1)


# testing
if __name__ == '__main__':
    graph = Graph()
    graph.insertVertex(0)
    graph.insertVertex(1)
    graph.insertVertex(2)
    graph.insertVertex(3)
    graph.insertVertex(4)
    graph.insertVertex(5)
    graph.insertVertex(6)
    graph.insertVertex(7)
    graph.addEdge(0, 1)
    graph.addEdge(1, 2)
    graph.addEdge(2, 1)
    graph.addEdge(2, 3)
    graph.addEdge(2, 5)
    graph.addEdge(3, 4)
    graph.addEdge(4, 1)
    graph.addEdge(5, 6)
    graph.addEdge(6, 7)
    graph.addEdge(7, 5)
    print(graph.detectCycle())
    graph.deleteVertex(4)
    print(graph.detectCycle())
    graph.deleteVertex(5)
    print(graph.detectCycle())
    graph.deleteVertex(1)
    print(graph.detectCycle())
