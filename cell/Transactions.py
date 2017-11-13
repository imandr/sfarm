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
    
    def __init__(self, owner, path, version, expiration):
        self.ID = Transaction.next_id()
        self.Path = path
        self.Version = version
        self.Notify = [owner]
        self.Expiration = expiration
        self.URLHead = owner.URLHead
        self.Started = False
        
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
    @property
    def expired(self):
        return self.Expiration is not None and self.Expiration < time.time() 
        
    def url(self):
        raise NotImplementedError
        
class GetTransaction(Transaction):
    
    def __init__(self, owner, path, version, expiration):
        Transaction.__init__(self, owner, path, version, expiration)
        
    def url(self):
        self.URL = "%s/get/%s/%s" % (self.URLHead, self.ID, self.Path)
        
class PutTransaction(Transaction):
    def __init__(self, owner, path, version, size, relicas, expiration)
        Transaction.__init__(self, owner, path, version, expiration)
        self.Size = size
        self.Replicas = replicase

    def url(self):
        self.URL = "%s/put/%s" % (self.URLHead, self.ID)
        
class ReplicateTransaction(Transaction):
    def __init__(self, owner, path, version, relicas)
        Transaction.__init__(self, owner, path, version, None)
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
    def getTransaction(self, path, version):
        t = GetTransaction(self, path, version, time.time() + self.ExpirationInterval)
        self.Transactions[t.ID] = t
        return t
        
    @synchronized
    def putTransaction(self, path, version, size, replicas):
        t = PutTransaction(self, path, version, size, replicas, time.time() + self.ExpirationInterval)
        self.Transactions[t.ID] = t
        return t
        
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
                del self.Requests[rid]
                
