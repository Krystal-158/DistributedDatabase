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
        # transaction - site map (txId: set of sites which it accessed)
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
        self.txSite[txId] = set()

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
        # all operations not in the waitlist must have been executed
        # # execute ops not in the waitlist and not executed
        # if commit:
        #     for op in tx.ops:  
        #         if not op.exec:
        #             for siteId in range(1, 11):
        #                 self.sites[siteId].execute(op, tx)  
                  
        # all ops executed, commit them all
        if commit:
            for op in tx.ops:
                for siteId in range(1, 11):
                    if not self.sites[siteId].commit(op, tx):
                        commit = False
                        break
                if not commit:
                    break
        # release all the locks
        for op in tx.ops:
            lock = Lock(txId, op.varId, op.opType)
            for siteId in op.locks:
                self.sites[siteId].ReleaseLock(lock)
            self.execWaitlist(lock)                
        # delete the tx from self.transactions
        del self.transactions[txId]
        # delete the tx from self.txSite
        self.txSite.pop(txId)
        # delete the tx from self.graph
        self.graph.deleteVertex(txId)
        return commit
    
    def execWaitlist(self, lock):
        """
        Apply a recently-released lock to the first operation needed it in the waitlist
        then execute the operation, if there's any
        """
        for op in self.waitlist:
            if op.varId == lock.variable_id:
                # the first op in the waitlist waiting for the lock
                requireLock = True # if failed to acquire a lock (site not fail and has the variable, i.e. lock is hold by other op)
                tx = self.transactions[op.txId]
                # try to get locks from all sites except for failed ones
                for siteId in range(1, 11):
                    getLock, err = self.sites[siteId].ApplyLock(lock)
                    if not getLock and err != 1:
                        # fail to get a lock and the site is available
                        requireLock = False
                        break
                # if all need lock acquired, try to execute the op
                if requireLock:
                    if op.opType == 'read':
                        for siteId in range(1, 11):
                            if self.sites[siteId].execute(op, tx):
                                op.exec = True
                                break
                    else:
                        executed = True
                        for siteId in range(1, 11):
                            if not self.sites[siteId].execute(op, tx):
                                executed = False
                                break
                        if executed:
                            op.exec = True           
                if op.exec:
                    # op executed, remove it from the waitlist
                    self.waitlist.remove(op)
                    # add the site which this op accessed into its site map
                    if op.varId % 2 == 0:
                        for siteId in range(1, 11):
                            self.txSite[op.txId].add(siteId)
                    else:
                        self.txSite[op.txId].add(op.varId % 10 + 1)
                break


    def readOp(self, txId, varId):
        """Read the value of a variable
        INPUT: txId(transaction id), varId(index of the variable which the operation wants to access)
        OUTPUT:
        If the op can require its lock immediately, execute it
        Else add the op into waitlist
        """
        # use current time as operation id
        op = Operation(txId, datetime.now(), 'read', varId)
        tx = self.transactions[txId]
        self.transactions[txId].addOp(op)
        # try to acquire lock
        lock = Lock(txId, varId, 'read')
        if varId % 2 == 0:
            
        else:
            siteId = varId % 10 + 1
            getLock, err = self.sites[siteId].ApplyLock(lock):
            if getLock:
                # lock acquired, try to execute it
                if self.sites[siteId].execute(op, tx):
                    op.exec = True
            elif not getLock and err == 1:
                # didn't get a lock due to site failure
                op.exec = True
        if not op.exec:


    def writeOp(self, txId, opId, varId, value):
        """Write the value to a variable
        INPUT: txId(transaction id), varId(index of variable which operation wants to access)
        OUTPUT:
        If the op can require its lock immediately, execute it
        Else add the op into waitlist
        """
        op = Operation(txId, opId, 'write', varId, value)
        self.transactions[txId].addOp(op)