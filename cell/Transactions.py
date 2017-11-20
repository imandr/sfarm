from threads import Primitive, MyThread, synchronized
from threading import Lock
import time

class Transaction(Primitive):
    
    NextID = 1
    NextIDLock = Lock()

    @classmethod
    def next_id(cls):
        with cls.NextIDLock:
            i = cls.NextID
            cls.NextIDLock += 1
            return i
    
    def __init__(self, owner, info, expiration):
        self.ID = Transaction.next_id()
        self.Info = info
        self.Notify = [owner]
        self.Expiration = expiration
        self.URLHead = owner.URLHead
        self.Started = False
        owner.addTransaction(self)
        self.Owner = owner
        
    @synchronized
    def addNotify(self, who):
        if not who in self.Notify:
            self.Notify.append(who)

    @synchronized
    def start(self):
        self.Started = True
        
    @property
    def started(self):
        return self.Started
    
    @synchronized
    def commit(self):
        assert self.started
        for n in self.Notify:
            n.commitTransaction(self)
        self.Notify = []
        self.Owner.removeTransaction(self)
        self.Owner = None
        
    @synchronized
    def rollback(self, typ, value, tback):
        assert self.started
        for n in self.Notify:
            n.rollbackTransaction(self)
        self.Notify = []
        self.Owner.removeTransaction(self)
        self.Owner = None
        
    @synchronized
    def cancel(self):
        assert not self.started
        self.Notify = []
        self.Owner.removeTransaction(self)
        self.Owner = None
        
    @synchronized
    @property
    def expired(self):
        return self.Expiration is not None and self.Expiration < time.time() 
        
    def url(self):
        raise NotImplementedError
        
class GetTransaction(Transaction):
    
    def __init__(self, owner, info, expiration):
        Transaction.__init__(self, owner, info, expiration)
        
    @property
    def url(self):
        return "%s/get/%s/%s" % (self.URLHead, self.ID, self.Info.lastName)
        
    class FileOpenForRead:
        
        def __init__(self, txn):
            self.Txn = txn
            self.F = self.Owner.openFile(txn.Info, "r")
            
        def __enter__(self):
            self.Txn.start()
            
        def __exit__(self, typ, value, tback):
            self.F.close()
            self.Txn.commit()
            self.Txn = None
            
        def read(self, size):
            return self.F.read(size)
            
    def open(self):
        return FileOpenForRead(self)
        
class PutTransaction(Transaction):
    def __init__(self, owner, info, relicas, expiration)
        Transaction.__init__(self, owner, info, expiration)
        self.Replicas = replicase

    @property
    def url(self):
        return "%s/put/%s" % (self.URLHead, self.ID)
        
    class FileOpenForWrite:
        
        def __init__(self, txn):
            self.Txn = txn
            self.F = self.Owner.openFile(txn.Info, "w")
            
        def __enter__(self):
            self.Txn.start()
            
        def __exit__(self, typ, value, tback):
            self.F.close()
            if typ:
                self.Txn.rollback(typ, value, tback)
            else:
                self.Txn.commit()
            self.Txn = None
            
        def write(self, block):
            return self.F.write(block)
            
    def open(self):
        return FileOpenForWrite(self)

class ReplicateTransaction(Transaction):
    def __init__(self, owner, info, relicas)
        Transaction.__init__(self, owner, info, None)
        self.Replicas = replicas
    
class TransactionOwner(Primitive):
    
    def __init__(self, config):
        MyThread.__init__(self)
        self.Transactions = {}          # request id -> request
        self.Config = config
        self.ExpirationInterval = 10.0          # get from config
        self.Terminate = False
        self.URLHead = config.URLHead
        
    @synchronized
    def transaction(self, tid):
        return self.Transactions.get(tid)
        
    @synchronized
    def addTransaction(self, t):
        self.Transactions[t.ID] = t
        
    @synchronized
    def removeTransaction(self, t):
        try:    
            del self.Transactions[t.ID]
        except KeyError:    
            pass

    @synchronized
    def purgeTransactions(self):
        for tid, t in self.Transactons.items():
            if not t.started and t.expired:
                t.cancel()
                del self.Transactions[tid]
                
