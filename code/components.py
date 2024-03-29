"""Component.py includes basic conceptions of components of a database.
Five components are:
    Class Site: the site where variables are placed. Site takes care of lock management,
                varaible management(read and write) and site status management.
    Class Variable: a class of variable. It has methods to read, write and commmit.
    Class Operation: an operation to be execute, including read and write operations.
    Class Transaction: a transaction is a list of operations. It has methods to add or
                       delete operations.
    Class Lock: a class of the lock, including read and write lock.

Contribution of authors:
    Yubing Bai: Class Variable,  Class Site, Class Lock
    Xiaowen Yan: Class Operation, Class Transaction

The details of classes and methods are specified below every definition of them.
"""

from datetime import datetime
debugMode = False

class Site:
    """Site is a place saving a list of variables.
    args:
        site_id: the id of site
        status: the status of site: 'available', 'fail'
        variable_list: a list of variables on this site
        lock_table: the locks applied on every variable.
    """
    def __init__(self, site_id):
        self.site_id = site_id
        self.status = "available"  # status: available, fail
        self.variable_list = dict()
        self.lock_table = dict()   # a dictionary of list of locks

        # initializes the vairables in this site
        for i in range(1, 21):
            if i%2 == 0:
                self.variable_list[i] = Variable(i, 10*i, 10*i)
                self.lock_table[i] = list()
            elif i%10 + 1 == self.site_id:
                self.variable_list[i] = Variable(i, 10*i, 10*i)
                self.lock_table[i] = list()

    def ApplyLock(self, lock, force=False):
        """Apply a lock on the variable.
        Input:
            force: True means if required lock already in hand, apply or upgrade directly.

        Output:
            -1: apply lock successfully.
            -2: is able to get the required lock, but not yet apply.
            0: lock conflicts, cannot apply
            1: site fail, cannot apply
            2: var does not exist on this site, cannot apply
            3: cannot apply read lock on duplicated var on recovered site.
        """
        if self.status == "fail":
            # False
            return 1

        vid = lock.variable_id
        if vid not in self.variable_list:
            if debugMode:
                print ("Cannot lock variable because variable {} does not exist on site {}! ".format(lock.variable_id, self.site_id))
            # False
            return 2

        if self.variable_list[vid].is_recovered:
            if lock.lock_type == "read" and vid % 2 == 0:
                # False
                if debugMode:
                    print("Recovered site hasn't been written yet.")
                return 3

        if self.variable_list[vid].lock_status == "free":
            self.variable_list[vid].lock_status = lock.lock_type
            self.lock_table[vid].append(lock)
            # True
            return -1
        elif self.variable_list[vid].lock_status == "write":
            #if lock.lock_type == "write" and (self.lock_table[vid])[0].transaction_id == lock.transaction_id:
            if (self.lock_table[vid])[0].transaction_id == lock.transaction_id:
                if debugMode:
                    print("Lock existed.")
                # True
                if force:
                    return -1
                else:
                    return -2
            else:
                if debugMode:
                    print ("Cannot lock variable because variable {} on site {} has write lock on! ".format(lock.variable_id, self.site_id))
                # False
                return 0
        elif self.variable_list[vid].lock_status == "read":
            if lock.lock_type == "write":
                if len(self.lock_table[vid]) == 1 and (self.lock_table[vid])[0].transaction_id == lock.transaction_id:
                    if force:
                        self.variable_list[vid].lock_status = lock.lock_type
                        self.lock_table[vid].clear()
                        self.lock_table[vid].append(lock)
                        if debugMode:
                            print("Upgrade read lock to write lock.")
                        return -1
                    # True
                    return -2
                if debugMode:
                    print ("Cannot apply WRITE lock because variable {} on site {} has read lock on! ".format(lock.variable_id, self.site_id))
                # False
                return 0
            else:
                for l in self.lock_table[vid]:
                    if l.transaction_id == lock.transaction_id:
                        if debugMode:
                            print("Lock existed.")
                        # True
                        if force:
                            return -1
                        else:
                            return -2
                self.lock_table[vid].append(lock)
                # True
                return -1
        else:
            if debugMode:
                print("Wrong lock status: ", self.variable_list[vid].lock_status)
            return False

    def ReleaseLock(self, lock):
        """Release lock on variable.
        If release a write lock: set lock status of the variable to free;
        If release a read lock: remove only the read lock of this transaction.
        Input:
            lock: The lock to be removed.
        Output:
            0: lock released successfully.
            1: lock released because ite failed
            2: variable not found on this site.
            3: lock already released because write lock covered this read lock.
            4: lock not found on this variable.
        """
        if debugMode:
            print("site{}: ".format(self.site_id), end="\t")
        if self.status == "fail":
            if debugMode:
                print("lock released because site failed. ")
            return 1
        
        vid = lock.variable_id
        if vid not in self.variable_list:
            if debugMode:
                print ("Cannot unlock variable because variable {} does not exist on site {}! ".format(lock.variable_id, self.site_id))
            return 2
        
        if lock.lock_type == "read" and  (self.variable_list[vid]).lock_status == "write":
            if lock.transaction_id == (self.lock_table[vid])[0].transaction_id:
                if debugMode:
                    print("lock already released because write lock covered read lock. ")
                return 3
            
        res = -1
        for i in self.lock_table[vid]:
            if i == lock:
                self.lock_table[vid].remove(lock)
                res = 0
        
        if  len(self.lock_table[vid]) == 0:
            self.variable_list[vid].lock_status = "free"

        if res == 0:
            if debugMode:
                print("Lock released.")
        else:
            if debugMode:
                print("Did not find this lock.")
            res = 4

        return res

    def fail(self):
        """Fail a site.
        Release all the locks on the site.
        """
        self.status = "fail"
        for vlocklist in self.lock_table.values():
            vlocklist.clear()
        for vid in self.variable_list:
            self.variable_list[vid].lock_status = "free"
    
    def recover(self):
        """Recover a site.
        Undo all uncommited operations and label variable as recovered.
        """
        self.status = "available"
        for v in self.variable_list.values():
            v.value = v.get_commited_value()
            v.is_recovered = True

    def dump_all(self, is_commited = True):
        """Print all the variables on this site in order of ascending index.
        Input:
            is_commited: whether you want the lastest commited value.
        """
        print("site {} -".format(self.site_id), end = " ")
        for vid, var in self.variable_list.items():
            if is_commited:
                print("x{}: {},".format(vid, var.get_commited_value()), end=" ")
            else:
                print("x{}: {},".format(vid, var.value), end=" ")
        print("")


    
    def read(self, variable_id, is_commited = False):
        """Read the value of the requested variable.
        Input:
            is_commited: whether you want the lastest commited value.
        Output:
            A tuple of [is_read_successful, the value of variable if read successfully]
        """
        if variable_id not in self.variable_list:
            return False, 0
        if self.status == "fail":
            return False, 0
        elif self.variable_list[variable_id].is_recovered == True:
            if variable_id%2 == 0:
                return False, 0
            else:
                if is_commited:
                    return True, self.variable_list[variable_id].get_commited_value()
                else:
                    return True, self.variable_list[variable_id].value
        else:
            if is_commited:
                return True, self.variable_list[variable_id].get_commited_value()
            else:
                return True, self.variable_list[variable_id].value


    def execute(self, operation, transaction):
        """Execute given operation.
        Input:
            operation: the operation to execute
            transaction: the transaction that the operation belongs to
        Output:
            Whether the operation execute successfully.
        """
        v_id = operation.varId
        o_type = operation.opType
        t_type = transaction.txType
        if self.status == "fail":
            if debugMode:
                print("Failed: Site {} is a failed site.".format(self.site_id))
            return False
        if v_id not in self.variable_list:
            if debugMode:
                print ("Failed: Variable {} does not exist on site {}! ".format(v_id, self.site_id))
            return False
        if t_type == "RO":
            t_time = transaction.startTime
            if o_type == "read":
                if self.variable_list[v_id].is_recovered == True and v_id%2 == 0:
                # cannot read duplicated(even-index) variables
                    if debugMode:
                        print("Failed: read duplicated variable {} on recovery site {}.".format(v_id, self.site_id))
                    return False
                print("T{} read last COMMITTED variable {} on site{} returns {}.".format(
                    transaction.txId, v_id, self.site_id, self.variable_list[v_id].get_commited_value(t_time)))
                return True
            else:
                if debugMode:
                    print("Failed: write operation exists in RO transaction.")
                return False
        elif t_type == "RW":
            if self.variable_list[v_id].is_recovered == True:
                if o_type == "read":
                # cannot read duplicated(even-index) variables
                    if v_id%2 == 0:
                        if debugMode:
                            print("Failed. read duplicated variable {} on recovery site {}".format(v_id, self.site_id))
                        return False

                    print("T{} read variable {} on site{} returns {}.".format(
                    transaction.txId, v_id, self.site_id, self.variable_list[v_id].value))
                    return True

                elif o_type == "write":
                    self.variable_list[v_id].set_value(operation.val)
                    if debugMode:
                        print("Done. T{} write value {} to variable {} on site{}.".format(
                    transaction.txId, self.variable_list[v_id].value, v_id, self.site_id))
                    return True
                else:
                    if debugMode:
                        print("Failed: wrong operation type: {}".format(o_type))
                    return False

            elif self.status == "available":
                if o_type == "read":
                    print("T{} read variable {} on site{} returns {}.".format(
                    transaction.txId, v_id, self.site_id, self.variable_list[v_id].value))
                    return True
                elif o_type == "write":
                    self.variable_list[v_id].set_value(operation.val)
                    if debugMode:
                        print("Done. T{} write value {} to variable {} on site{}".format(
                    transaction.txId, self.variable_list[v_id].value, v_id, self.site_id))
                    return True
                else:
                    if debugMode:
                        print("commit failed: wrong operation type: {}".format(o_type))
                    return False

            else:
                if debugMode:
                    print("Failed: wrong site status: {}".format(self.status))
                return False
        else:
            if debugMode:
                print("Wrong transaction type: {}".format(t_type))
            return False


    def commit(self, operation, transaction):
        """Commit an operation.
        Read operations do not need commit and always return True.
        Input:
            operation: the operation to commit.
            transaction: the transaction that the operation belongs to
        Output:
            Whether the operation commits successfully.
        """
        v_id = operation.varId
        o_type = operation.opType
        t_type = transaction.txType
        if self.status == "fail":
            if debugMode:
                print("Commit failed: Site {} is a failed site".format(self.site_id))
            return False
        
        if v_id not in self.variable_list:
            if debugMode:
                print ("Commit failed: Variable {} does not exist on site {}! ".format(v_id, self.site_id))
            return False
        
        if o_type == "read" or t_type == "RO":
            return True
        elif t_type == "RW":
            if self.variable_list[v_id].is_recovered == True:
                if o_type == "write":
                # set is_recovered to False
                    self.variable_list[v_id].commit()
                    self.variable_list[v_id].is_recovered = False
                    if debugMode:
                        print("commit done. T{} commit value {} to RECOVERED variable {} on site{}.".format(
                    transaction.txId, self.variable_list[v_id].get_commited_value(), v_id, self.site_id))
                    return True
                else:
                    if debugMode:
                        print("commit failed: wrong operation type: {}".format(o_type))
                    return False

            elif self.status == "available":
                if o_type == "write":
                    self.variable_list[v_id].commit()
                    if debugMode:
                        print("commit done. T{} commit value {} to variable {} on site{}".format(
                    transaction.txId, self.variable_list[v_id].get_commited_value(), v_id, self.site_id))
                    return True
                else:
                    if debugMode:
                        print("commit failed: wrong operation type: {}".format(o_type))
                    return False

            else:
                if debugMode:
                    print("commit failed: wrong site status: {}".format(self.status))
                return False
        else:
            if debugMode:
                print("Wrong transaction type: {}".format(t_type))
            return False

    def undo(self, operation):
        """Undo an operation on this site.
        Input:
            operation: the operation to undo.
        Output:
            Whether the operation is undone successfully.
        """
        v_id = operation.varId
        o_type = operation.opType
        if debugMode:
            print("Undo on site {}".format(self.site_id), end=" ")

        if self.status == "fail":
            if debugMode:
                print("failed: site failed")
            return False

        if v_id not in self.variable_list:
            if debugMode:
                print ("passed: Variable {} does not exist on the site! ".format(v_id))
            return True

        if o_type == "read":
            if debugMode:
                print ("passed: Read operation does not need undo! ")
            return True

        self.variable_list[v_id].undo()
        if debugMode:
            print ("succeeded.")
        return True
         

class Variable:
    """a class of the variable.
    args:
        variable_id: id of the variable
        value: value of the variable
        commited_value: a dict of (commited_time, commited_value)
        lock_status: lock status on this variable: 'free', 'read', 'write'
        is_recovered: whether the variable is recently recovered and yet has no write commit.
    """
    def __init__(self, variable_id, value, c_value):
        self.variable_id = variable_id
        self.value = value
        self.commited_value = dict()
        self.commited_value[datetime.now()] = c_value
        self.lock_status = "free"
        self.is_recovered = False

    def set_value(self, value):
        """write value.
        Input:
            value: the value to write.
        """
        self.value = value

    def commit(self):
        """commit the current value.
        set commited value as current value.
        """
        self.commited_value[datetime.now()]  = self.value
        
    def get_commited_value(self, time = None):
        """Get lastest commited value before given time.
        Input:
            time: the timepoint before which we want the lastest commited value.
        returns:
            the lastest commited value if time is None, 
            otherwise the lastest commited value before time.
        """
        if not time:
            time = datetime.now()
        tmax = datetime.fromtimestamp(0)
        res = None
        for t, v in self.commited_value.items():
            if t > tmax and t<=time:
                res = v
                tmax = t
        return res
    
    def undo(self):
        """undo value.
        Cover the value by lastest commited value.
        """
        self.value = self.get_commited_value()
        
class Lock:
    def __init__(self, transaction_id, variable_id, lock_type):
        """a class of the lock
        args:
            transaction_id: the transaction which wants to apply this lock.
            variable_id: the varaible on which the lock is applied
            lock_type: the type of the lock: 'read', 'write'
        """
        self.transaction_id = transaction_id
        self.variable_id = variable_id
        self.lock_type = lock_type

    def __eq__(self,other):
        """Overwrite the equal operation.
        """
        res = (self.transaction_id==other.transaction_id ) and (self.variable_id==other.variable_id ) and (self.lock_type==other.lock_type )
        return res

class Operation:
    """definition of an operation
    args:
        opType: read, write
        varId: variable_id
        val: value to write if any
        opId: the id of operation
        txId: the transaction that operation belongs to
        exec: whether this operation has been executed
        locks: a list of locks acquired by this operation
    """
    def __init__(self, txId, opType, varId, val=None):
        self.opType = opType # 'read' or 'write'
        self.varId = varId
        self.val = val
        self.opId = datetime.now()
        self.txId = txId
        self.exec = False
        self.locks = list() # locks acquired (represented by site index)

class Transaction:
    """definition of a transaction: a list of operations
    args:
        txId: transaction ID
        txType:  RO, RW
        abort: whether the transaction need to abort
        ops: a list of operations of this transaction
        startTime: the start time of the transaction
        accessedFailedSite: a list of sites that ever failed after the transaction accessed them.
    """
    def __init__(self, txId, txType = "RW"):
        self.txId = txId
        self.txType = txType
        self.abort = False
        self.ops = list()
        self.startTime = datetime.now()
        self.accessedFailedSite = list()

    def addOp(self, op):
        """Add operation to the transaction.
        Input:
            op: the operation to add.
        """
        if op not in self.ops:
            self.ops.append(op)

    def clearOps(self):
        """Clear up the operation list.
        """
        self.ops = list()