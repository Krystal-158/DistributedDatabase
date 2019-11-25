# site_status: available, fail, recovery
# lock_status: free, read, write
# lock_type: read, write
class Site:
    def __init__(self, site_id):
        self.site_id = site_id
        self.status = "avaliable"  # site_status: available, fail, recovery
        self.variable_list = dict()
        self.lock_table = dict()   # a dictionary of queue of locks

        # initializes the vairables in this site
        for i in range(1, 21):
            if i%2 == 0:
                self.variable_list[i] = Variable(i, 10*i)
                self.lock_table[i] = list()
            elif i%10 + 1 == self.site_id:
                self.variable_list[i] = Variable(i, 10*i)
                self.lock_table[i] = list()
                
    def ApplyLock(self, lock):
        """Apply a lock on the variable.
        1. check if there is write lock on variable. if so, lock apply fails.
        2. check if there is read lock on variable. if so, applying read lock continues, applying write lock fails.
        2.1 apply read lock: check if transaction already holds read lock on this variable, if not, apply one.
        3. no lock on the variable: apply the lock on the variable
        """
        if lock.variable_id not in self.variable_list:
            print ("Cannot lock variable because variable {} does not exist on site {}! ".format(lock.variable, self.site_id))
            return False
        
        vid = lock.variable_id
        if self.variable_list[vid] == "free":
            self.variable_list[vid].lock_status = lock.type
            self.lock_table[vid].append(lock)
            return True
        elif self.variable_list[vid].lock_status == "write":
            print ("Cannot lock variable because variable {} on site {} has write lock on! ".format(lock.variable, self.site_id))
            return False
        elif self.variable_list[vid].lock_status == "read":
            if lock.lock_type == "write":
                print ("Cannot apply WRITE lock because variable {} on site {} has read lock on! ".format(lock.variable, self.site_id))
                return False
            else:
                for l in self.lock_table[vid]:
                    if (l.transaction_id == lock.transaction_id)
                        return True
                self.lock_table[vid].append(lock)
                return True
            
    def ReleaseLock(self, lock):
        """Release lock on variable.
        If release a write lock: set variable to free;
        If release a read lock: remove the read lock of this transaction and check if there are more read locks in the queue.
        """
        if lock.variable_id not in self.variable_list:
            print ("Cannot unlock variable because variable {} does not exist on site {}! ".format(lock.variable, self.site_id))
            return False
        vid = lock.variable_id
        if lock.lock_type != self.variable_list[vid].lock_status:
            print ("Cannot unlock variable because variable {} does not has {} lock on site {}! ".format(lock.variable, lock.lock_type, self.site_id))
            return False
        
        for i in self.lock_table[vid]:
            if i.transaction_id == lock.transaction_id:
                self.lock_table[vid].remove(lock)
        if  len(self.lock_table[vid]) == 0:
            self.variable_list[vid].lock_status == "free"
        return True

    def fail(self):
        self. status = "fail"
        for vlocklist in self.lock_table.values():
            vlocklist.clear()
    
    def recover(self):
        self.status = "recovery"
    
    def dump(self, variable_id):
        """return the value of the requested variable."""
        if variable_id not in self.variable_list:
            return False
        if self.status == "fail":
            

class Variable:
    def __init__(self, variable_id, value):
        self.variable_id = variable_id
        self.value = value
        self.commited_value = value
        self.lock_status = "free"  # lock_status: free, read, write
        
class Lock:
    def __init__(self, lock_id, transaction_id, variable_id, lock_type):
        self.transaction_id = transaction_id
        self.variable_id = variable_id
        self.lock_type = lock_type  # lock_type: read, write

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
