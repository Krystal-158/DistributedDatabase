"""TransactionManager.py implement the transaction manager and functions to execute operations.
Basic operation execution methods are:
    startTx
    endTx
    readOp
    writeOp
    exetWaitlist
    dumpOp
    failOp
    recoverOp

Contribution of authors:
    Yubing Bai: dumpOp(), failOp(), recoverOp()
    Xiaowen Yan: all other parts

The details of methods are specified below every definition of them.
"""
from graph import Graph
from components import Site, Variable, Lock, Operation, Transaction, debugMode
from datetime import datetime

class TransactionManager:
    """Transaction manager takes care of operation execution.
    args:
        sites: 10 sites (site index: )
        varSite: varSite (variable index: list of site indexes where it's stored)
        transactions: transactions (transaction index: transaction)
        txSite: transaction - siteId map (txId: set of ID of sites which it accessed)
                add a siteId into tx's site list when the tx gets a lock on the site and execute an op
                when a site fails, abort all txs which accessed it
        graph: graph for deadlock check
        waitlist: list of operations which haven't got required lock yet
    """
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
        INPUT: 
            txType (transaction type: RW/RO), txId (transaction id)
        """
        print('Start T{}'.format(txId))
        self.transactions[txId] = Transaction(txId, txType)
        self.graph.insertVertex(txId)
        self.txSite[txId] = set()

    def endTx(self, txId):
        """End a transaction: commit or abort
        If the transacton hasn't aborted yet, 
        check if all operations in the transaction got required locks.
        If so, execute those ops which haven't been executed, then commit 
        else abort.
        At the end, delete the transaction from self.transactions, self.graph, self.txSite
        Release all the locks and assign them to ops in the waitlist if possible

        INPUT: 
            txId(transaction id)
        OUTPUT: 
            True - commit, False - abort
        """
        if debugMode:
            print("Try to end transaction ", txId)
        tx = self.transactions[txId]
        # check if the transaction aborted previously (due to site failure or deadlock)
        if tx.abort:
            print("T{} Aborted because it accessed site {} and it failed later.".format(txId, tx.accessedFailedSite))
            commit = False
        else:
            commit = True
            for op in tx.ops:
                # at least one operation hasn't got its lock
                if op in self.waitlist:
                    commit = False
                    print("T{} aborted because it failed to get all required locks to work.".format(txId))
                    break
                  
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
        else:
            # if tx aborts, undo all the write operations
            for op in tx.ops:
                if op.opType == 'write' and op.exec:
                    for siteId in op.locks:
                        self.sites[siteId].undo(op)
            # if tx aborts, remove all the ops in the waitlist            
            waitingOps = list()
            for op in self.waitlist:
                if op.txId == tx.txId:
                    waitingOps.append(op)
            for op in waitingOps:
                self.waitlist.remove(op)
        if commit:
            for op in tx.ops:
                if op.opType == 'write':
                    print("T{} wrote {} to variable {} to sites {}.".format(op.txId, op.val, op.varId, op.locks))
        # release all the locks
        accessedVar = set()
        for op in tx.ops:
            lock = Lock(txId, op.varId, op.opType)
            if debugMode:
                print("Operation is holding lock on ", op.locks)
            for siteId in op.locks:
                self.sites[siteId].ReleaseLock(lock)
            accessedVar.add(op.varId)
        for var in accessedVar:
            if debugMode:
                print("Finding ops waiting for variable ", var)
                print("Start executing waitlist.")
            self.execWaitlist(var)              
        op.locks = list()           
        # delete the tx from self.transactions
        self.transactions.pop(txId)
        # delete the tx from self.txSite
        self.txSite.pop(txId)
        # delete the tx from self.graph
        self.graph.deleteVertex(txId)
        if commit:
            print("T{} Committed".format(txId))
        # else:
        #     print("T{} Aborted".format(txId))
        return commit
    
    def execWaitlist(self, varId):
        """Execute operations in the waitlist if possible
        Apply a recently-released lock to the first operation needed it in the waitlist
        then execute the operation, if there's any
        There are three conditions that the next operation in the waitlist 
        with the same varId can be possibly executed after the first one:
            1. the first and next op are both read
            2. the first and next op come from the same tx
        
        INPUT:
            varId(index of the variable whose lock could be assigned to an op in the waitlist)
        """
        execAgain = False
        for op in self.waitlist:
            if op.varId == varId:
                # the first op in the waitlist waiting for the lock
                getLock = True # if failed to acquire a lock (site not fail and has the variable, i.e. lock is hold by other op)
                tx = self.transactions[op.txId]
                if tx.txType == 'RW':
                    # try to get locks from all sites except for failed ones
                    getLock = self.acquireLock(op, True)
                    # if all need lock acquired, try to execute the op
                    if getLock and len(op.locks) > 0:
                        if debugMode:
                            print("All locks acquired, try to execute operation {} variable {} value {}".format(op.opType, op.varId, op.val))
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
                else:
                    # the first operation in the waitlist is from RO tx
                    # just execute it
                    if debugMode:
                        print("Operation belongs to tx {}, which is read-only, no need to acquire lock.".format(tx.txId))
                    for siteId in self.varSite[op.varId]:
                        if self.sites[siteId].execute(op, tx):
                            op.exec = True
                            break
                    self.waitlist.remove(op)
                    # as the released lock is actually not assigned to a new op 
                    execAgain = True
                    if debugMode:
                        print("Current op belongs to read-only tx, will continue to execute waitlist.")
                    break
                if op.exec:
                    # op executed, remove it from the waitlist
                    self.waitlist.remove(op)
                    # no need to update the graph                        
                    # add the site which this op accessed into its site map
                    for siteId in op.locks:
                        self.txSite[op.txId].add(siteId)
                    for waitOp in self.waitlist:
                        if waitOp.varId == varId:
                            if op.opType == 'read':
                                if waitOp.opType == 'read' or waitOp.txId == op.txId:
                                    if debugMode:
                                        if waitOp.opType == 'read':
                                            print("Current op and next op are both read, will continue to execute waitlist.")
                                        elif waitOp.txId == op.txId:
                                            print("Next op belongs to the same tx as current one, will continue to execute waitlist.")
                                    execAgain = True
                            elif waitOp.txId == op.txId:
                                if debugMode:
                                    print("Current op and next op come from the same tx, will continue to execute waitlist.")
                                execAgain = True
                        break
                    break
        if execAgain:
            self.execWaitlist(varId)

    def readOp(self, txId, varId):
        """Read the value of a variable
        If the op can require its lock immediately, execute it
        Else add the op into waitlist

        INPUT: 
            txId(transaction id), varId(index of the variable which the operation wants to access)
        """
        op = Operation(txId, 'read', varId)
        tx = self.transactions[txId]
        tx.addOp(op)
        getLock = True
        # try to acquire lock
        if self.transactions[txId].txType == 'RW':
            getLock = self.acquireLock(op)
            if getLock and len(op.locks) > 0:
                # lock acquired, try to execute it
                for siteId in op.locks:
                    if self.sites[siteId].execute(op, tx):
                        op.exec = True
                        self.txSite[op.txId].add(siteId)
                        break
        else:
            # execute RO operations immediately
            for siteId in self.varSite[op.varId]:
                if self.sites[siteId].execute(op, tx):
                    op.exec = True
                    break
        # if the operation is not executed, add it to the waitlist
        if not op.exec:
            self.waitlist.append(op) 
            # update the graph-------------------------------------------------------------
            updated = False
            for waitOp in reversed(self.waitlist):
                # op.tx is waiting for waitOp.tx
                if waitOp.varId == op.varId and waitOp.txId != op.txId:
                    self.graph.addEdge(op.txId, waitOp.txId)
                    updated = True
                    break
            if not updated:
                # there's no operation from different tx waiting for the same lock
                # the op is waiting for the lock's current holder(s)
                for siteId in self.varSite[op.varId]:
                    for lockHolder in self.sites[siteId].lock_table[op.varId]:
                        self.graph.addEdge(op.txId, lockHolder.transaction_id)
            # check deadlock
            txCycle = self.graph.detectCycle()
            while txCycle:
                if len(txCycle) > 1:
                    # find the youngest transaction
                    youngest = txCycle[0].vId
                    if debugMode:
                        print("Deadlock detected: ", txCycle)
                    for t in txCycle:
                        if self.transactions[t.vId].startTime > self.transactions[youngest].startTime:
                            youngest = t.vId
                    # abort the youngest
                    self.abort(self.transactions[youngest]) 
                    txCycle = self.graph.detectCycle()
                else:
                    if debugMode:
                        print("No deadlock detected!")
                    break           

    def writeOp(self, txId, varId, value):
        """Write the value to a variable
        If the op can require its lock immediately, execute it
        Else add the op into waitlist

        INPUT: 
            txId(transaction id), varId(index of variable which operation wants to access)
        """
        op = Operation(txId, 'write', varId, value)
        tx = self.transactions[txId]
        tx.addOp(op)
        # try to acquire lock
        getLock = self.acquireLock(op)
        if getLock and len(op.locks) > 0:
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
            updated = False
            for waitOp in reversed(self.waitlist):
                # op.tx is waiting for waitOp.tx
                if waitOp.varId == op.varId and waitOp.txId != op.txId:
                    self.graph.addEdge(op.txId, waitOp.txId)
                    updated = True
                    break
            if not updated:
                # there's no operation from different tx waiting for the same lock
                # the op is waiting for the lock's current holder(s)
                for siteId in self.varSite[op.varId]:
                    for lockHolder in self.sites[siteId].lock_table[op.varId]:
                        self.graph.addEdge(op.txId, lockHolder.transaction_id)
            # check deadlock
            txCycle = self.graph.detectCycle()
            while txCycle:
                if len(txCycle) > 1:
                    # find the youngest transaction
                    youngest = txCycle[0].vId
                    if debugMode:
                        print("Deadlock detected: ", txCycle)
                    for t in txCycle:
                        if self.transactions[t.vId].startTime > self.transactions[youngest].startTime:
                            youngest = t.vId
                    # abort the youngest
                    self.abort(self.transactions[youngest]) 
                    txCycle = self.graph.detectCycle()
                else:
                    if debugMode:
                        print("No deadlock detected!")
                    break
    
    def acquireLock(self, op, waitlist=False):
        """Try to acquire all the locks

        INPUT:  
            op(operation acquiring lock), 
            force(True - force to acquire lock, False)
            waitlist(True - the op comes from the waitlist, default = False)
        OUTPUT: 
            True - all locks required, False - failed to acquire lock
        """
        lock = Lock(op.txId, op.varId, op.opType)
        getLock = True
        for siteId in self.varSite[op.varId]:
            err = self.sites[siteId].ApplyLock(lock) # (lock, waitlist)???
            if debugMode:
                print(err)
            if err == -1:
                # successfully acquired a lock
                op.locks.append(siteId)
            elif err == -2:
                # the current lock holder belongs to the same tx
                ddlk = False
                if not waitlist:
                    # the operation doesn't come from the waitlist
                    # see if there's an op from different tx waiting for this lock
                    for waitOp in self.waitlist:
                        #if waitOp.varId == op.varId and waitOp.txId != op.txId:
                        if waitOp.varId == op.varId:
                            ddlk = True
                        if ddlk:
                            if debugMode:
                                print("There is an op from different tx waiting for this lock, add op to waitlist")
                            getLock = False
                            break
                if not ddlk:
                    # there's no op from different tx waiting for this lock
                    # just force to acquire the lock
                    if debugMode:
                        print("Force to acquire locks")
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
            op.locks = list()
        return getLock

    def abort(self, tx):
        """Abort the transaction
        1. undo all executed ops
        2. remove all tx's operations from waitlist
        3. release all acquired locks
        4. delete tx from transactions and graph
        5. execute waitlist

        INPUT:
            tx(transaction which should abort)
        """
        # undo all tx's executed ops
        for op in tx.ops:
            if op.opType == 'write' and op.exec:
                for siteId in op.locks:
                    self.sites[siteId].undo(op)
        # remove all tx's operations from waitlist
        for op in self.waitlist:
            if op.txId == tx.txId:
                self.waitlist.remove(op)
        # release all acquired locks
        released = set()
        for op in tx.ops:
            if debugMode:
                print("Operation {} variable {} value {} is holding locks {}".format(op.opType, op.varId, op.val, op.locks))
            lock = Lock(tx.txId, op.varId, op.opType)
            for siteId in self.varSite[op.varId]:
                if self.sites[siteId].ReleaseLock(lock) == 0:
                    # sucessfully released a lock
                    if debugMode:
                        print("Variable {} at site {} released lock".format(op.varId, siteId))
                    released.add(lock.variable_id)
        # delete tx from transactions, txSite, and graph
        self.transactions.pop(tx.txId)
        self.txSite.pop(tx.txId)
        self.graph.deleteVertex(tx.txId)
        # execute waitlist
        for varId in released:
            if debugMode:
                print("Start executing waitlist.")
            self.execWaitlist(varId)
        print("T{} aborted due to deadlock".format(tx.txId))


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
        for txId, tx in self.transactions.items():
            if siteId in self.txSite[txId]:
                tx.abort = True
                tx.accessedFailedSite.append(siteId)
                # remove the site from op.locks
                for op in tx.ops:
                    if siteId in op.locks:
                        op.locks.remove(siteId)
        # site fails
        site = self.sites[siteId]
        site.fail()
        print("Site {} failed.".format(siteId))

    def recoverOp(self, siteId):
        """recover a site.
        INPUT: site id.
        """
        site = self.sites[siteId]
        if site.status == "fail":
            site.recover()
            # if odd-index variable exists on site, they become free after recovery.
            if siteId%2 == 0:
                if debugMode:
                    print("Start executing waitlist.")
                self.execWaitlist(siteId - 1)
                self.execWaitlist(siteId - 1 + 10)
            print("Site {} recovered.".format(siteId))
        else:
            print("Site does not fail.")





