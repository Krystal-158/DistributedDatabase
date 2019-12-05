from graph import Graph
from components import Site, Variable, Lock, Operation, Transaction, debugMode
from datetime import datetime

class TransactionManager:
    def __init__(self):
        # 10 sites (site index: )
        self.sites = dict() 
        # varSite (variable index: list of site indexes where it's stored)
        self.varSite = dict() 
        # transactions (transaction index: transaction)
        self.transactions = dict()
        # transaction - siteId map (txId: set of ID of sites which it accessed)
        # add a siteId into tx's site list when the tx gets a lock on the site and execute an op
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
        if debugMode:
            print("Try to end transaction ", txId)
        tx = self.transactions[txId]
        # check if the transaction aborted previously (due to site failure or deadlock)
        if tx.abort:
            if debugMode:
                print("Transaction aborted previously")
            commit = False
        else:
            commit = True
            for op in tx.ops:
                # at least one operation hasn't got its lock
                if op in self.waitlist:
                    commit = False
                    if debugMode:
                        print("Operation {} failed to get its lock".format(op.opId))
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
                for siteId in op.locks:
                    if not self.sites[siteId].commit(op, tx):
                        if debugMode:
                            print("Site {} commit failed".format(siteId))
                        commit = False
                        break
                if not commit:
                    break
        # release all the locks
        accessedVar = set()
        for op in tx.ops:
            lock = Lock(txId, op.varId, op.opType)
            for siteId in op.locks:
                self.sites[siteId].ReleaseLock(lock)
            accessedVar.add(op.varId)
        for var in accessedVar:
            self.execWaitlist(var)              
        op.locks = list()           
        # delete the tx from self.transactions
        del self.transactions[txId]
        # delete the tx from self.txSite
        self.txSite.pop(txId)
        # delete the tx from self.graph
        self.graph.deleteVertex(txId)
        if debugMode:
            if commit:
                print("Committed transaction ", txId)
            else:
                print("Aborted transaction ", txId)
        return commit
    
    def execWaitlist(self, varId):
        """
        Apply a recently-released lock to the first operation needed it in the waitlist
        then execute the operation, if there's any
        """
        for op in self.waitlist:
            if op.varId == varId:
                lock = Lock(op.txId, op.varId, op.opType)
                # the first op in the waitlist waiting for the lock
                getLock = True # if failed to acquire a lock (site not fail and has the variable, i.e. lock is hold by other op)
                tx = self.transactions[op.txId]
                # try to get locks from all sites except for failed ones
                for siteId in self.varSite[op.varId]:
                    err = self.sites[siteId].ApplyLock(lock)
                    if err == -1:
                        # successfully acquired a lock
                        op.locks.append(siteId)
                    elif err == -2:
                        # the current lock holder belongs to the same tx
                        # but the current op is the first one in the wl which has the same varId
                        # just force to acquire lock
                        _ = self.sites[siteId].ApplyLock(lock, True)
                        op.locks.append(siteId)
                    elif err == 0:
                        # there's other ops holding required lock
                        getLock = False
                        break
                # if all need lock acquired, try to execute the op
                if getLock and len(op.locks) > 0:
                    if debugMode:
                        print("All locks acquired, try to execute operation ", op.opId)
                    if op.opType == 'read':
                        for siteId in op.locks:
                            if self.sites[siteId].execute(op, tx):
                                op.exec = True
                                break
                    else:
                        executed = True
                        for siteId in op.locks:
                            if not self.sites[siteId].execute(op, tx):
                                executed = False
                                break
                        if executed:
                            op.exec = True           
                if op.exec:
                    # op executed, remove it from the waitlist
                    self.waitlist.remove(op)
                    # update the graph ------------------------------------------
                    # delete related edge
                    
                    # see if the op's tx has
                    for waitOp in self.transactions[op.txId]:

                    # add the site which this op accessed into its site map
                    for siteId in op.locks:
                        self.txSite[op.txId].add(siteId)
                break

    def readOp(self, txId, varId):
        """Read the value of a variable
        INPUT: txId(transaction id), varId(index of the variable which the operation wants to access)
        OUTPUT:
        If the op can require its lock immediately, execute it
        Else add the op into waitlist
        """
        op = Operation(txId, 'read', varId)
        tx = self.transactions[txId]
        tx.addOp(op)
        # try to acquire lock
        lock = Lock(txId, varId, 'read')
        getLock = True
        for siteId in self.varSite[varId]:
            err = self.sites[siteId].ApplyLock(lock)
            if err == -1:
                # successfully acquired a lock
                op.locks.append(siteId)
            elif err == -2:
                # the current lock holder belongs to the same tx
                # see if there's an op from different tx waiting for this lock
                ddlk = False
                for waitOp in self.waitlist:
                    if waitOp.varId == op.varId and waitOp.txId != op.txId:
                        ddlk = True
                        break
                    if ddlk:
                        if debugMode:
                            print("There is an op from different tx waiting for this lock, add op to waitlist")
                        getLock = False
                        break
                if not ddlk:
                    # there's no op from different tx waiting for this lock
                    # just force to acquire the lock
                    _ = self.sites[siteId].ApplyLock(lock, True)
                    op.locks.append(siteId)
            elif err == 0:
                # there's other ops holding required lock
                getLock = False
                break
        if not getLock:
            # there's other ops holding required lock, release those acquired
            for siteId in op.locks:
                self.sites[siteId].ReleaseLock(lock)
        elif len(op.locks) > 0:
            # lock acquired, try to execute it
            for siteId in op.locks:
                if self.sites[siteId].execute(op, tx):
                    op.exec = True
                    self.txSite[op.txId].add(siteId)
                    break
        # if the operation is not executed, add it to the waitlist
        if not op.exec:
            self.waitlist.append(op)
            # update the graph-------------------------------------------------------------


    def writeOp(self, txId, varId, value):
        """Write the value to a variable
        INPUT: txId(transaction id), varId(index of variable which operation wants to access)
        OUTPUT:
        If the op can require its lock immediately, execute it
        Else add the op into waitlist
        """
        op = Operation(txId, 'write', varId, value)
        tx = self.transactions[txId]
        tx.addOp(op)
        # try to acquire lock
        lock = Lock(txId, varId, 'write')
        getLock = True
        for siteId in self.varSite[varId]:
            err = self.sites[siteId].ApplyLock(lock)
            if err == -1:
                # successfully acquired a lock
                op.locks.append(siteId)
            elif err == -2:
                # the current lock holder belongs to the same tx
                # see if there's an op from different tx waiting for this lock
                ddlk = False
                for waitOp in self.waitlist:
                    if waitOp.varId == op.varId and waitOp.txId != op.txId:
                        ddlk = True
                        break
                    if ddlk:
                        if debugMode:
                            print("There is an op from different tx waiting for this lock, add op to waitlist")
                        getLock = False
                        break
                if not ddlk:
                    # there's no op from different tx waiting for this lock
                    # just force to acquire the lock
                    _ = self.sites[siteId].ApplyLock(lock, True)
                    op.locks.append(siteId)
            elif err == 0:
                # there's other ops holding required lock
                getLock = False
                break
        if not getLock:
            # there's other ops holding required lock, release those acquired
            for siteId in op.locks:
                self.sites[siteId].ReleaseLock(lock)
        elif len(op.locks) > 0:
            # lock acquired, try to execute it
            executed = True
            for siteId in op.locks:
                if not self.sites[siteId].execute(op, tx):
                    executed = False
                    break
            if executed:
                op.exec = True
                for siteId in op.locks:
                    self.txSite[op.txId].add(siteId)
        # if the operation is not executed, add it to the waitlist
        if not op.exec:
            self.waitlist.append(op)
            # update the graph

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
    	"""fail a site and abort all related transactions.
        INPUT: site id.
        """
    	# all related transactions fail.
    	for tx in self.transactions:
        	if siteId in self.txSite[tx]:
	            self.transactions[tx].abort = True

        # site fails
    	site = self.sites[siteId]
    	site.fail()
    	print("site {} failed.".format(siteId))

    def recoverOp(self, siteId):
    	"""recover a site.
        INPUT: site id.
        """
    	site = self.sites[siteId]
    	if site.status == "fail":
        	site.recover()
        	# if odd-index variable exists on site, they become free after recovery.
        	if siteId%2 == 0:
        		execWaitlist(siteId - 1)
        		execWaitlist(siteId - 1 + 10)
        	print("site {} recovered.".format(siteId))
    	else:
        	print("site does not fail.")





