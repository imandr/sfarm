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
        
    @synchronized
    def rollback(self):
        assert self.started
        for n in self.Notify:
            n.rollbackTransaction(self)
        self.Notify = []
        
    @synchronized
    def cancel(self):
        assert not self.started
        self.Notify = []
        
    @synchronized
    @property
    def expired(self):
        return self.Expiration is not None and self.Expiration < time.time() 
        
    def url(self):
        raise NotImplementedError
        
class GetTransaction(Transaction):
    
    def __init__(self, owner, info, expiration):
        Transaction.__init__(self, owner, info, expiration)
        
    def url(self):
        self.URL = "%s/get/%s/%s" % (self.URLHead, self.ID, self.Info.lastName)
        
class PutTransaction(Transaction):
    def __init__(self, owner, info, relicas, expiration)
        Transaction.__init__(self, owner, info, expiration)
        self.Replicas = replicase

    def url(self):
        self.URL = "%s/put/%s" % (self.URLHead, self.ID)
        
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
    def addTransaction(self, t):
        self.Transactions[t.ID] = t
        
    @synchronized
    def removeTransaction(self, tid):
        try:    
            del self.Transactions[tid]
        except KeyError:    
            pass

    @synchronized
    def purgeTransactions(self):
        for tid, t in self.Transactons.items():
            if not t.started and t.expired:
                t.cancel()
                del self.Transactions[tid]
                
