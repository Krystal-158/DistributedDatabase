class Site:
    """Site is a place saving a list of variables.
    args:
        status: available, fail
        lock_status: free, read, write
        lock_type: read, write
    """
    def __init__(self, site_id):
        self.site_id = site_id
        self.status = "avaliable"  # status: available, fail
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

    def ApplyLock(self, lock):
        """Apply a lock on the variable.
        0. if site fail. return [False, 1]
        1. check if varaible exists on the site, if not, return [false, 2]
        2. if lock duplicated var on recovered site, return [false, 3]
        3. check if there is lock conflicts, if so, return [false, 0]
            1) check if there is write lock on variable. if so, lock apply fails unless two lock belongs to one transaction.
            2) check if there is read lock on variable. if so:
            2.1) apply write lock: if two lock belongs to one transaction, upgrade read lock. otherwise fails.
            2.2) apply read lock: check if transaction already holds read lock on this variable, if not, apply one.
            3) no lock on the variable: apply the lock on the variable, return [True, -1]
        """
        if self.status == "fail":
            return False, 1

        vid = lock.variable_id
        if vid not in self.variable_list:
            print ("Cannot lock variable because variable {} does not exist on site {}! ".format(lock.variable, self.site_id))
            return False, 2

        if self.variable_list[vid].is_recovered:
            if lock.lock_type == "read" and vid % 2 == 0:
                return False, 3

        if self.variable_list[vid] == "free":
            self.variable_list[vid].lock_status = lock.lock_type
            self.lock_table[vid].append(lock)
            return True, -1
        elif self.variable_list[vid].lock_status == "write":
            if lock.lock_type == "write" and self.lock_table[vid][0].transaction_id == lock.transaction_id:
                print("Lock existed.")
                return True, -1
            else:
                print ("Cannot lock variable because variable {} on site {} has write lock on! ".format(lock.variable, self.site_id))
                return False, 0
        elif self.variable_list[vid].lock_status == "read":
            if lock.lock_type == "write":
                if len(self.lock_table[vid]) == 1 and self.lock_table[vid][0].transaction_id == lock.transaction_id:
                    self.variable_list[vid].lock_status = lock.lock_type
                    self.lock_table[vid].clear()
                    self.lock_table[vid].append(lock)
                    print("Upgrade read lock to write lock.")
                    return True, -1
                print ("Cannot apply WRITE lock because variable {} on site {} has read lock on! ".format(lock.variable, self.site_id))
                return False, 0
            else:
                for l in self.lock_table[vid]:
                    if l.transaction_id == lock.transaction_id:
                        print("Lock existed.")
                        return True, -1
                self.lock_table[vid].append(lock)
                return True, -1

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
        """recover a site.
        undo all uncommited operations and label variable as recovered.
        """
        self.status = "available"
        for v in self.variable_list.values():
            v.value = v.commited_value
            v.is_recovered = True

    def dump_all(self, is_commited = False):
        """return all the variables at this site in index order.
        args:
            is_commited: whether you want the lastest commited value.
        """
        print("site {} -".format(self.site_id), end = " ")
        if self.status == "fail":
            print("fail")
            return False

        for var in self.variable_list.values():
            if var.is_recovered == True:

        elif self.variable_list[variable_id].is_recovered == True:
            if variable_id%2 == 0:
                return False, 0
            else:
                if is_commited:
                    return True, self.variable_list[i].commited_value
                else:
                    return True, self.variable_list[i].value
        else:
            if is_commited:
                return True, self.variable_list[i].commited_value
            else:
                return True, self.variable_list[i].value


    
    def read(self, variable_id, is_commited = False):
        """return the value of the requested variable.
        args:
            is_commited: whether you want the lastest commited value.
        return value is a tuple of [is_dump_successful, the value of variable if dump successfully]
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
                    return True, self.variable_list[i].commited_value
                else:
                    return True, self.variable_list[i].value
        else:
            if is_commited:
                return True, self.variable_list[i].commited_value
            else:
                return True, self.variable_list[i].value


    def execute(self, operation, transaction):
        """execute operatin
        """
        v_id = operation.obj
        o_type = operation.opType
        t_type = transaction.txType
        if self.status == "fail":
            print("Failed: Site {} is a failed site".format(self.site_id))
            return False, 0
        if t_type == "RO":
            if o_type == "read":
                if self.variable_list[v_id].is_recovered == True and v_id%2 == 0:
                # cannot read duplicated(even-index) variables
                    print("Failed: read duplicated variable {} on recovery site {}".format(v_id, self.site_id))
                    return False
                print("Done. T{} read last COMMITED variable {} on site{} returns {}".format(
                    transaction.txID, v_id, self.site_id, self.variable_list[v_id].commited_value))
                return True
            else:
                print("Failed: write operation exists in RO transaction.")
                return False
        elif t_type == "RW":
            if self.variable_list[v_id].is_recovered == True:
                if o_type == "read":
                # cannot read duplicated(even-index) variables
                    if v_id%2 == 0:
                        print("Failed. read duplicated variable {} on recovery site {}".format(v_id, self.site_id))
                        return False
                    # ################ something could goes wrong

                    print("T{} read variable {} on site{} returns {}".format(
                    transaction.txID, v_id, self.site_id, self.variable_list[v_id].value))
                    return True

                elif o_type == "write":
                    self.variable_list[v_id].set_value(operation.val)
                    print("Done. T{} write value {} to variable {} on site{}.".format(
                    transaction.txID, self.variable_list[v_id].commited_value, v_id, self.site_id))
                    return True
                else:
                    print("Failed: wrong operation type: {}".format(o_type))
                    return False

            elif self.status == "available":
                if o_type == "read":
                    print("T{} read variable {} on site{} returns {}".format(
                    transaction.txID, v_id, self.site_id, self.variable_list[v_id].value))
                    return True
                elif o_type == "write":
                    self.variable_list[v_id].set_value(operation.val)
                    print("Done. T{} write value {} to variable {} on site{}".format(
                    transaction.txID, self.variable_list[v_id].commited_value, v_id, self.site_id))
                    return True
                else:
                    print("commit failed: wrong operation type: {}".format(o_type))
                    return False

            else:
                print("Failed: wrong site status: {}".format(self.status))
                return False
        else:
            print("Wrong transaction type: {}".format(t_type))
            return False


    def commit(self, operation, transaction):
        """commit an operation and return whether commit successfully.
        """
        v_id = operation.obj
        o_type = operation.opType
        t_type = transaction.txType
        if self.status == "fail":
            print("Commit failed: Site {} is a failed site".format(self.site_id))
            return False
        if o_type == "read" or t_type == "RO":
            return True
        elif t_type == "RW":
            if self.variable_list[v_id].is_recovered == True:
                if o_type == "write":
                # set is_recovered to False
                    self.variable_list[v_id].commit()
                    self.variable_list[v_id].is_recovered == False
                    print("commit done. T{} commit value {} to RECOVERED variable {} on site{}.".format(
                    transaction.txID, self.variable_list[v_id].commited_value, v_id, self.site_id))
                    return True
                else:
                    print("commit failed: wrong operation type: {}".format(o_type))
                    return False

            elif self.status == "available":
                if o_type == "write":
                    self.variable_list[v_id].commit()
                    print("commit done. T{} write value {} to variable {} on site{}".format(
                    transaction.txID, self.variable_list[v_id].commited_value, v_id, self.site_id))
                    return True
                else:
                    print("commit failed: wrong operation type: {}".format(o_type))
                    return False

            else:
                print("commit failed: wrong site status: {}".format(self.status))
                return False
        else:
            print("Wrong transaction type: {}".format(t_type))
            return False
            

class Variable:
    """a class for a variable.
    args:
        lock_status: free, read, write
    """
    def __init__(self, variable_id, value, c_value):
        self.variable_id = variable_id
        self.value = value
        self.commited_value = c_value
        self.lock_status = "free"
        self.is_recovered = False

    def set_value(self, value):
        self.value = value

    def commit(self)
        self.commited_value = self.value
        
class Lock:
    """a class of lock
    args:
        lock_type: read, write
    """
    def __init__(self, lock_id, transaction_id, variable_id, lock_type):
        self.transaction_id = transaction_id
        self.variable_id = variable_id
        self.lock_type = lock_type

class Operation:
    """
    definition of an operation
    args:
        opType: read, write
    """
    def __init__(self, txId, opId, opType, varId, val=None):
        self.opType = opType
        self.varId = varId
        self.val = val
        self.opId = opId
        self.txId = txId
        self.exec = False


  
class Transaction:
    """
    definition of a transaction: a list of operations
    args:
        txType:  RO, RW
        obj: variable_id
    """
    def __init__(self, txId, txType = ""):
        self.txId = txId
        self.txType = txType
        self.abort = False
        self.ops = list()

    def addOp(self, op):
        if op not in self.ops:
            self.ops.append(op)

    def clearOps(self):
        self.ops = list()
