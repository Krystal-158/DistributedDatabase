"""graph.py defines the graph used to detect deadlocks.

Contribution of authors:
    Implemented by Xiaowen Yan

The details of functions are specified below every definition of them.
"""
from queue import Queue
debugMode = True

class Vertex:
    """ Vertex is a point in the graph
    args:
        vid: the index of the vertex
        visited: if it's visited in a dfs
        adj: set of its neighbours
    """
    def __init__(self, vId):
        self.vId = vId
        self.visited = 0 # flag used in cycle detection
        self.adj = set() # adjacent list

    def addAdj(self, v):
        """Add Vertex v into current adjacent list

        INPUT: 
            v(the vertex which will be added to self.adj)
        """
        self.adj.add(v)

    def deleteAdj(self, v):
        """Remove a vertex from adjacent list

        INPUT: 
            v(the vertex which will be deleted from adjacent list)
        """
        self.adj.discard(v)


class Graph:
    """ Graph contains vertices and edges
    Each vertex in the graph represents a transaction. If there's an edge 
    pointing from T1 to T2, then T2 is in T1's adjacent list while T1 
    isn't in T2's, which means T1 is waiting for T2.
    args:
        vertices: list of vertices in the graph
    """
    def __init__(self):
        self.vertices = list()

    def insertVertex(self, vId):
        """Add a vertex if it's not in the graph

        INPUT:
            vId(index of vertex to be inserted)
        """
        v = Vertex(vId)
        if v not in self.vertices:
            self.vertices.append(v)
    
    def getVertex(self, vId):
        """Find a particular vertex in the graph

        INPUT:
            vId(index of vertex to be found)
        OUTPUT:
            v(vertex found), None is not found
        """
        for v in self.vertices:
            if v.vId == vId:
                return v
        return None

    def deleteVertex(self, vId):
        """Delete a vertex from the graph, thus delete all corresponding edges.

        INPUT:
            vId(index of the vertex to be deleted)
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
        """Add vertex uId to vertex vId's adjacent list
        Then there's an edge pointing from v to u
        
        INPUT:
            uId(index of the current lock holder), vId(index of the waiting tx)
        """
        v = self.getVertex(vId)
        u = self.getVertex(uId)
        v.addAdj(u)

    def deleteEdge(self, vId, uId):
        """Delete vertex uId from vertex vId's adjacent list

        INPUT:
            vId(index of the vertex whose adjacent list to be updated)
            uId(index of the vertex to be deleted from v's adj list)
        """
        v = self.getVertex(vId)
        u = self.getVertex(uId)
        v.deleteAdj(u)

    def detectCycle(self):
        """Detect if there's a cycle in the graph.
        If so, return a list of vertices in the cycle, otherwise return an empty list

        OUTPUT:
            cycle(containing all vertices in a cycle)
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
        """Depth-first search a vertex
        
        INPUT:
            v(the vertex to be searched)
            stack(containing vertices visited so far)
            cycle(containing all vertices in a cycle)
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
