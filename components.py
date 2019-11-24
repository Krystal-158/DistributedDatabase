#!

class Operation:
    """
    definition of an operation
    """
    def __init__(self, opType):
        self.opType = opType

    
class Transaction:
    """
    definition of a transaction: a list of operations
    """
    def __init__(self, txId):
        self.txId = txId
        self.ops = list()

    def addOp(self, op):
        if op not in self.ops:
            self.ops.append(op)

    def clearOps(self):
        self.ops = list()