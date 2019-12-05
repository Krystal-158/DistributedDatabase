from graph import Graph
from components import Site, Variable, Lock, Operation, Transaction
from datetime import datetime

class TransactionManager:
    def __init__(self):
        # 10 sites (site index: )
        self.sites = dict() 
        # varSite (variable index: list of site indexes where it's stored)
        self.varSite = dict() 
        # transactions (transaction index: transaction)
        self.transactions = dict()
        # transaction - site map (txId: list of sites which it accessed)
        # add a site into tx's site list when the tx gets a lock on the site and execute an op
        # when a site fails, abort all txs which accessed it
        self.txSite = dict()
        # graph for deadlock check
        self.graph = Graph()
        # list of operations which haven't got required lock yet
        self.waitlist = list()
        # odd indexed variables are at one site each
        # even indexed variables are at all sites
        for varIndex in range(1, 21):
            self.varSite[varIndex] = list()
            if varIndex % 2 == 0:
                for j in range(1, 11):
                    self.varSite[varIndex].append(j)
            else:
                self.varSite[varIndex].append(varIndex % 10 + 1)
        for siteIndex in range(1, 11):
            self.sites[siteIndex] = Site(siteIndex) # initialize the sites

    def startTx(self, txType, txId):
        """Start a transaction
        INPUT: txType (transaction type: RW/RO), txId (transaction id)
        OUTPUT:
        """
        print('Start transaction ', txId)
        self.transactions[txId] = Transaction(txId, txType)
        self.graph.insertVertex(txId)
        self.txSite[txId] = list()

    def endTx(self, txId):
        """End a transaction: commit or abort
        INPUT: txId(transaction id)
        OUTPUT: True - commit, False - abort

        If the transacton hasn't aborted yet, 
        check if all operations in the transaction got required locks.
        If so, execute those ops which haven't been executed, then commit 
        else abort.
        At the end, delete the transaction from self.transactions, self.graph, self.txSite
        Release all the locks and assign them to ops in the waitlist if possible
        """
        tx = self.transactions[txId]
        # check if the transaction aborted previously (due to site failure or deadlock)
        if tx.abort:
            commit = False
        else:
            commit = True
            for op in tx.ops:
                # at least one operation hasn't got its lock
                if op in self.waitlist:
                    commit = False
        # all operations got their locks, try to execute ops not executed yet
        if commit:
            for op in tx.ops:
                # not yet executed
                if not op.exec:
                    if op.varId % 2 == 0:
                        if op.opType == 'read':
                            # read from one site
                            for siteId in range(1, 11):     
                                if self.sites[siteId].execute(op, tx):
                                    op.exec = True
                                    break
                        else:
                            # write to all sites
                            for siteId in range(1, 11):
                                if not self.sites[siteId].execute(op, tx):
                                    # if any site fails to write, commit is false
                                    commit = False
                            # after writing to all sites, set the op executed
                            if commit:
                                op.exec = True
                    else:
                        siteId = op.varId % 10 + 1
                        if op.opType == 'read':
                            if self.sites[siteId].execute(op, tx):
                                op.exec = True 
                        else:
                            if not self.sites[siteId].execute(op, tx):
                                commit = False
                            else:
                                op.exec = True 
        # all ops executed, commit them all
        if commit:
            for op in tx.ops:
                if op.varId % 2 == 0:
                    if op.opType == 'read':
                        for siteId in range(1, 11):
                            if self.sites[siteId].commit(op, tx):
                                break
                    else: 
                        for siteId in range(1, 11):
                            if not self.sites[siteId].commit(op, tx):
                                commit = False
                                break
                else:
                    siteId = op.varId % 10 + 1
                    if not self.sites[siteId].commit(op, tx):
                        commit = False
        # release all the locks
        for op in tx.ops:
            lock = Lock(txId, varId, op.opType)
            if op.varId % 2 == 0:
                for siteId in range(1, 11):
                    self.sites[siteId].ReleaseLock(lock)
            else:
                siteId = op.varId % 10 + 1
                self.sites[siteId].ReleaseLock(lock)
            self.reapplyLock(lock)                
        # delete the tx from self.transactions
        del self.transactions[txId]
        # delete the tx from self.txSite
        self.txSite.pop(txId)
        # delete the tx from self.graph
        self.graph.deleteVertex(txId)
        return commit
    
    def reapplyLock(self, lock):
        """
        Apply a recently-released lock to the first operation needed it in the waitlist
        then execute the operation, if there's any
        """
        for op in self.waitlist:
            if op.varId == lock.variable_id:
                if op.varId % 2 == 0:

                else:


    def readOp(self, txId, opId, varId):
        """Read the value of a variable
        INPUT: txId(transaction id), varId(index of the variable which the operation wants to access)
        OUTPUT:
        If the op can require its lock immediately, execute it
        Else add the op into waitlist
        """
        op = Operation(txId, opId, 'read', varId)
        self.transactions[txId].addOp(op)


    def writeOp(self, txId, opId, varId, value):
        """Write the value to a variable
        INPUT: txId(transaction id), varId(index of variable which operation wants to access)
        OUTPUT:
        If the op can require its lock immediately, execute it
        Else add the op into waitlist
        """
        op = Operation(txId, opId, 'write', varId, value)
        self.transactions[txId].addOp(op)

    def dumpOp(self, dumpsites = None):
    	"""query for all the variable on all the site.
    	OUTPUT: print all the variables on all sites in order of ascending index.
    	"""
    	if dumpsites:
    		for sid in dumpsites.sort():
    			self.sites[sid].dump_all()
    	else:
	    	for site in self.sites.values():
	    		site.dump_all()
	def failOp(self, siteId):
		return

	def recoverOp(self, recoverOp):
		return



