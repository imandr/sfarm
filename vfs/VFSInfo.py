import time, json
import serialize

def VFSCanonicPath(path):
	if path[:1] != '/':
		path = '/' + path
	while "//" in path:
		path = path.replace('//', '/')
	if path.endswith("/"):
		path = path[:-1]
	return path		
    
canonicPath = VFSCanonicPath

class	VFSInfo:
	Version = '1.0'
	def __init__(self, typ, path):
		self.Type = typ
		self.Path = VFSCanonicPath(path)
		self.Owner = None
		self.Prot = 'rwr-'
		self.Attrs = {}
		self.Flags = 0
        self.CTime = time.time()
        self.Size = 0

    @staticmethod
    def fromDict(dct):
        typ = dct["Type"]
        path = dct["Path"]
        if typ == "d":
            info = VFSDirInfo(path)
        elif typ == "f":
            info = VFSFileInfo(path)
        else:
            raise ValueError("Unknown item type '%s'" % (typ,))
        info.Owner = dct["Owner"]
        info.Prot = dct["Protection"]
        info.Attrs = dct["Attributes"]
        info.Flags = dct["Flags"]
        info.CTime = dct["CTime"]
        info.Size = dct["Size"]
        info.fillFromDict(dct)
        return info
        
    def fillFromDict(self, dct):            # overridable
        pass        

    @staticmethod
    def fromJSON(text):
        return VFSInfo.fromDict(json.loads(text))

    def toDict(self):
        return dict(
            Type = self.Type,
            Path = self.Path,
            Owner = self.Owner,
            Protection = self.Prot,
            Attributes = self.Attrs,
            Flags = self.Flags,
            CTime = self.Ctime,
            Size = self.Size
        )
        
    def toJSON(self):
        return json.dumps(self.toDict())

	def __getitem__(self, attr):
		try:				return	self.Attrs[attr]
		except KeyError:	return None

	def __setitem__(self, attr, val):
		self.Attrs[attr] = val

	def __delitem__(self, attr):
		try:	del self.Attrs[attr]
		except KeyError:	pass
	
	def attributes(self):
		return self.Attrs.keys()

	def dataClass(self):
		return self['__data_class'] or '*'
        
    @property
    def lastName(self):
        return self.Path.split("/")[-1]

class	VFSFileInfo(VFSInfo):
	FLAG_ESTIMATE_SIZE = 1

	def __init__(self, path, version = None):
		VFSInfo.__init__(self, 'f', path)
		self.Servers = []
		self.Size = None
        self.Version = None
        
    def fillFromDict(self, dct):
        self.Version = dct.get("Version")
        self.Servers = dct.get("Servers",[])
        self.Size = dct.get("Size")    

    def toDict(self):
        dct = VFSInfo.toDict(self)
        dct["Version"] = self.Version
        dct["Servers"] = self.Servers
        dct["Size"] = self.Size

	def sizeMB(self):
		if self.Size and self.Size > 0:
			return long(float(self.Size)/1024/1024 + 0.5)
		else:
			return 0L

	def sizeEstimated(self):
		return (self.Flags & self.FLAG_ESTIMATE_SIZE) != 0

	def setActualSize(self, size):
		self.Flags = self.Flags & ~self.FLAG_ESTIMATE_SIZE
		self.Size = long(size)
	
	def setSizeEstimate(self, size):
		self.Size = long(size)
		self.Flags = self.Flags | self.FLAG_ESTIMATE_SIZE
	
	def isStoredOn(self, srv):
		return srv in self.Servers
		
	def addServer(self, srv):
		if not srv in self.Servers:
			self.Servers.append(srv)

	def removeServer(self, srv):
		while srv in self.Servers:
			self.Servers.remove(srv)
	
	def mult(self):
		return len(self.Servers)
		
	def isStored(self):
		return self.mult() > 0

		
class	VFSDirInfo(VFSInfo):
	def __init__(self, path, str = None):
		VFSInfo.__init__(self, path, 'd', str)
		
