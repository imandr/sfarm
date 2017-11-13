from threads import Primitive, MyThread
from threading import Lock
import time

class Transaction(object):
    
    NextID = 1
    NextIDLock = Lock()

    @classmethod
    def next_id(cls):
        with cls.NextIDLock:
            i = cls.NextID
            cls.NextIDLock += 1
            return i
    
    def __init__(self, keeer, path, version, expiration):
        self.ID = Transaction.next_id()
        self.Path = path
        self.Version = version
        self.Keeper = keeper
        self.Expiration = expiration
        
    def done(self):
        self.Keeper.removeTransaction(self.ID)
        self.Keeper = None
        
    @property
    def expired(self):
        return self.Expiration is not None and self.Expiration < time.time() 
        
    def url(self):
        raise NotImplementedError
        
class GetTransaction(Transaction):
    
    def __init__(self, keeer, path, version, expiration):
        Transaction.__init__(self, keeer, path, version, expiration)
        
    def url(self):
        self.URL = "%s/get/%s/%s" % (self.Keeper.URLHead, self.ID, self.Path)
        
class PutTransaction(Transaction):
    def __init__(self, keeer, path, version, size, relicas, expiration)
        Transaction.__init__(self, keeer, path, version, expiration)
        self.Size = size
        self.Replicas = replicase

    def url(self):
        self.URL = "%s/put/%s" % (self.Keeper.URLHead, self.ID)
        
class ReplicateTransaction(Transaction):
    def __init__(self, keeer, path, version, relicas)
        Transaction.__init__(self, keeer, path, version, None)
        self.Replicas = replicas
    
class TransactionKeeper(MyThread):
    
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
    def purge(self):
        for tid, t in self.Transactons.items():
            if t.expired:
                del self.Requests[rid]
                
    def run(self):
        while not self.Terminate:
            self.purge()
            time.sleep(10)
