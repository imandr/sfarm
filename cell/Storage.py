from threads import Primitive, MyThread, synchronized, Task
from random import shuffle
from logfile import LogFile

class ReplicatorTask(Task):
    
    def run(self, transaction):
        assert isinstance(transaction, ReplicateTransaction)
        
        

class Storage(Primitive):

    def __init__(self, config):
        Primitive.__init__(self)
        # read PSAs from config
        self.PSAs = {}
        self.ReplicatorQueue = {}

    @synchronized
    def findVersion(self, lpath, version, delete_if_differs = False):
        for psa in self.PSAs.values():
            info = psa.findFile(lpath, version = version, delete_if_differs = delete_if_differs):
            if info is not None:
                return info, psa
        return None, None
        
    @synchronized
    def findFile(self, lpath):
        for psa in self.PSAs.values():
            info = psa.findFile(lpath):
            if info:
                return info, psa
        return None, None
        
    @synchronized
    def getTransaction(self, lpath, version):
        info, psa = self.findVersion(lpath, verison, delete_if_differs=True)
        if info:
            return psa.getTransaction(info)
            
    @synchronized
    def putTransaction(self, lpath, version, size, replicas):
        info, psa = self.findFile(lpath)
        if info is not None:
            return None
        psas = self.PSAs.values()
        random.shuffle(psas)            # apply preferences here
        info = VFSFileInfo(lpath, version)
        info.setActualSize(size)
        for psa in psas:
            if psa.canReceive(info):
                t = psa.putTransaction(self, info, replicas)
                t.addNotify(self)
                
    @synchronized
    def commitTransaction(self, txn):
        assert isinstance(txn, PutTransaction)
        info = txn.Info
        replicas = txn.Replicas
        rt = self.replicateTransaction(info.Path, info.Version, replicas)
        
    @synchronized
    def replicateTransaction(self, lpath, version, replicas):
        info, psa = self.findVersion(lpath, verison, delete_if_differs=True)
        if info is not None:
            return psa.replicateTransaction(lpath, version, replicas)
        
    @synchronized
    def deleteVersionsExcept(self, lpath, version_to_keep):
        self.findVersion(lpath, version_to_keep, delete_if_differs=True)
        
        
class PSA(TransactionOwner, MyThread):

	def __init__(self, name, config, attractors):	# size in mb
        TransactionOwner.__init__(self, config)
		self.Name = name
		self.Root = config.get("PSA %" % (name,), "root")
		self.Attractors = attractors
		self.DataRoot = self.Root + '/data'
		self.InfoRoot = self.Root + '/info'
		self.Used = 0L
		self.LastPrune = 0
        self.TransactionExpiration = 5.0            # get from config

	def log(self, msg):
		msg = 'PSA[%s@%s]: %s' % (self.Name, self.Root, msg)
		if LogFile:
			LogFile.log(msg)
		else:
			print msg
			sys.stdout.flush()

	def fullDataPath(self, lpath):
		if not lpath or lpath[0] != '/':
			lpath = '/' + lpath
		return self.DataRoot + lpath

	def fullInfoPath(self, lpath):
		if not lpath or lpath[0] != '/':
			lpath = '/' + lpath
		return self.InfoRoot + lpath

	def logPath(self, fpath):
		if fpath[:len(self.DataRoot)] == self.DataRoot:
			tail = fpath[len(self.DataRoot):]
		elif fpath[:len(self.InfoRoot)] == self.InfoRoot:
			tail = fpath[len(self.InfoRoot):]
		if not tail:	tail = '/'
		return tail

	def dirPath(self, path):
		dp = string.join(string.split(path,'/')[:-1],'/')
		#if not dp:	dp = '/'
		return dp

	def calcUsed(self):
		used = 0
		for lp, i in self.listFiles():
			if i:
				used = used + i.sizeMB()
		return used

	def spaceUsage(self):
		return float(self.Used + self.reservedByTxns())/float(self.Size)

	def free(self): 	# calculate actual physical space available
		return min(free_MB(self.Root), self.Size - self.Used) - self.reservedByTxns()
		
	def freeMB(self):
		return free_MB(self.Root)


    #
    # Storage Interface
    #
    
	def findFile(self, lpath, version = None, delete_if_differs = False):
		info = self.getFileInfo(lpath)
        if info is None:    return None
        if version is not None and version != info.Version: 
            if delete_if_differs:
                self.delFile(lpath)
            return None
        return info

	def canReceiveFile(self, info):
		return self.free() >= info.sizeMB() and self.attractor(info.dataClass()) > 0

    def getTransaction(self, info):
        return GetTransaction(self, info, time.time() + self.TransactionExpiration)
        
    def putTransaction(self, info, replicas):
        return PutTransaction(self, info, replicas, time.time() + self.TransactionExpiration)

    @synchronized
    def commitTransaction(self, txn):
        if isinstance(txn, PutTransaction):
            info = txn.Info
            self.storeFileInfo(info.Path, info)
            # start replication here
            
    @synchronized
    def rollbackTransaction(self, txn):
        if isinstance(txn, PutTransaction):
            self.delFile(txn.Info.Path)

    def openFile(self, info, mode):
        return open(self.fullDataPath(info.Path), mode)
        
	def storeFileInfo(self, lpath, info):
		ipath = self.fullInfoPath(lpath)
		try:	os.makedirs(self.dirPath(ipath),0711)
		except: pass
		open(ipath,'w').write(info.toJSON())
		# set file owner here
				
	def getFileInfo(self, lpath):
		ipath = self.fullInfoPath(lpath)
		dpath = self.fullDataPath(lpath)
		try:	st = os.stat(dpath)
		except: 
			self.delFile(lpath)
			return None
		if stat.S_ISDIR(st[stat.ST_MODE]):
			try:	os.rmdir(dpath)
			except: pass
			try:	os.rmdir(ipath)
			except: pass
			return None
		try:	f = open(ipath,'r')
		except:
			#self.delFile(lpath)
			return None
		else:
			str = f.read()
			f.close()
			info = VFSInfo.fromJSON(str)
            assert info.Type == 'f' and info.Path == lpath
		return info

	def listRec(self, list, dir):
		# run recursively through info records
		for fn in glob.glob1(dir,'*'):
			fpath = dir + '/' + fn
			try:	st = os.stat(fpath)
			except: continue
			if stat.S_ISDIR(st[stat.ST_MODE]):
				list = self.listRec(list, fpath)
			else:
				lpath = self.logPath(fpath)
				info = self.getFileInfo(lpath)
				list.append((lpath, info))
		return list

	def listFiles(self):
		list = []
		return self.listRec(list, self.InfoRoot)

	def init(self):
		# create PSA directories
		try:	os.makedirs(self.DataRoot, 0711)
		except OSError, val:
			if val.errno == errno.EEXIST:
				st = os.stat(self.DataRoot)
				if not stat.S_ISDIR(st[stat.ST_MODE]):
					raise OSError, val
			else:
				raise OSError, val
		try:	os.makedirs(self.InfoRoot, 0711)
		except OSError, val:
			if val.errno == errno.EEXIST:
				st = os.stat(self.InfoRoot)
				if not stat.S_ISDIR(st[stat.ST_MODE]):
					raise OSError, val
			else:
				raise OSError, val
		# remove data entries which do not have infos
		data_lst = []
		data_lst = self.listRec(data_lst, self.DataRoot)
		for lpath, info in data_lst:
			if not info:
				self.log('CellStorage.init: deleted data for: %s' % lpath)
				self.delFile(lpath)

		# calc how much space is in use
		self.Used = self.calcUsed()

	def delFile(self, lpath):
		lpath = self.canonicPath(lpath)
		try:	
			st = os.stat(self.fullDataPath(lpath))
		except:
			pass
		else:
			sizemb = int(float(st[stat.ST_SIZE])/1024/1024 + 0.5)
			self.Used = self.Used - sizemb
		try:	os.remove(self.fullDataPath(lpath))
		except: pass
		try:	os.remove(self.fullInfoPath(lpath))
		except: pass
		dp = self.dirPath(lpath)
		while dp and dp != '/':
			try:	os.rmdir(self.fullDataPath(dp))
			except: pass
			try:	os.rmdir(self.fullInfoPath(dp))
			except: pass
			dp = self.dirPath(dp)
		
	def receive(self, lpath, info):
		# create new file, data (0 size) and info
		# set file UID
		# create txn
		#try:	
		#	pwrec = pwd.getpwnam(info.Username)
		#	uid = pwrec[2]
		#	gid = pwrec[3]
		#except:
		#	return None
		dpath = self.fullDataPath(lpath)
		try:	os.makedirs(self.dirPath(dpath), 0711)
		except: pass		# it may already exist

		try:	
			os.close(os.open(dpath,os.O_CREAT, 0744))
		except:
			return None
		# self.storeFileInfo(lpath, info)
		# set UID here
		return PutTxn(info.sizeMB(), self, lpath, info)

	def receiveComplete(self, lpath, info):
		self.log('Received: %s' % lpath)
		self.storeFileInfo(lpath, info)

	def receiveAborted(self, lpath):
		self.log('Receive aborted: %s' % lpath)
		try:	os.remove(self.fullDataPath(lpath))
		except: pass
		
	def send(self, lpath):
		return GetTxn(self, lpath)

	def status(self):
		# returns size, used, reserved, physically free
		return self.Size, self.Used, self.reservedByTxns(), self.freeMB()

	def fileBeingReceived(self, lpath):
		for txn in self.txnList():
			if txn.isPutTxn() and txn.LPath == lpath:
				return 1

	def replicate(self, nfrep):
		for lpath, info in self.listFiles():
			if info != None:
				cellmgr_global.DataMover.replicate(nfrep, 
					self.fullDataPath(lpath),
					lpath, info)

	def idle(self):
		if time.time() > self.LastPrune + 3600:
			self.prune()
		self.LastPrune = time.time()

	def prune(self):
		# remove empty directories from Info and Data area
		self.log("Pruning")
		self.pruneRec(self.DataRoot)
		self.pruneRec(self.InfoRoot)
		
	def pruneRec(self, dir):
		for fn in glob.glob1(dir,'*'):
			fpath = dir + '/' + fn
			try:	st = os.stat(fpath)
			except: continue
			if stat.S_ISDIR(st[stat.ST_MODE]):
				self.pruneRec(fpath)
				try:	os.rmdir(fpath)
				except:
					self.log("can not remove directory <%s>: %s %s" %
						(fpath, sys.exc_type, sys.exc_value))
				else:
					self.log("removed directory <%s>" % fpath)

	def attractor(self, sclass):
		if self.Attractors.has_key(sclass):
			return self.Attractors[sclass]
		if self.Attractors.has_key('*'):
			return self.Attractors['*']
		return 0

