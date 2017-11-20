import socket
from cStringIO import StringIO
import cPickle
from thread import MyThread


MAXMSG = 10000

def packMessage(*msg, **args):
    return cPickle.dumps((msg, args))

def unpackMessage(txt):
    msg, args = cPickle.loads(txt)
    return msg, args
        


class CellBroadcastRequest(object):

    def __init__(self, addr):
        self.BAddr = addr

    def getFile(self, info, wait=5.0):
        assert info.Version is not None
        sock = socket.socket(sock.AF_INET, socket.SOCK_DRGAM)
        dgram = self.packMessage("get", info.Path, info.Version)
        sock.sendto(dgram, self.BAddr)
        sock.settimeout(wait)
        try:    
            msg, addr = sock.recvfom(MAXMSG)
        except socket.timeout:
            return None
        (reply, url), args = self.unpackMessage(msg)
        assert reply == "get"
        return url
        
    def putFile(self, info, replicas, wait=5.0):
        sock = socket.socket(sock.AF_INET, socket.SOCK_DRGAM)
        dgram = self.packMessage("put", info.Path, info.Version, info.Size, replicas)
        sock.sendto(dgram, self.BAddr)
        sock.settimeout(wait)
        try:    
            msg, addr = sock.recvfom(MAXMSG)
        except socket.timeout:
            return None
        (reply, url), args = self.unpackMessage(msg)
        assert reply == "put"
        return url
        
    def removeVersions(self, lpath, except_version):
        sock = socket.socket(sock.AF_INET, socket.SOCK_DRGAM)
        dgram = self.packMessage("rm", lpath, except_version)
        sock.sendto(dgram, self.BAddr)
        
class Cell(MyThread):

    def __init__(self, port, storage, request_keeper):
        self.Port = port
        self.Storage = storage
        self.Terminate = False
        self.RequestKeeper = request_keeper
        
    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", self.Port))
        
        while not self.Terminate:
            msg, addr = sock.recvfrom(MAXMSG)
            msg, args = unpackMessage(msg)
            reply = None
            if msg == "get":
                reply = self.getFile(*msg[:1], **args)
            elif msg == "put":
                reply = self.putFile(*msg[:1], **args)
            elif msg == "rm":
                reply = self.removeVersionsExcept(*msg[:1], **args)
            if reply is not None:
                if not isinstance(reply, tuple):    reply = (reply,)
                reply = (msg,) + reply
                sock.sendto(packMessage(*reply), addr)
                
    def getFile(self, path, version):
        t = self.Storage.getTransaction(path, version)
        if t is not None:
            return t.url
        
    def putFile(self, path, version, size, replicas):
        t = self.Storage.putTransaction(path, version, size, replicas)
        if t is not None:
            return t.url
    
    def removeVersionsExcept(self, path, except_version):
        self.Storage.deleteVersionsExcept(path, except_version)
        
class CellServerApp(WSGIApp):
    
    def __init__(self, request, handler_class, storage):
        WSGIApp.__init__(request, handler_class)
        self.Storage = storage
        
class CellServerHandler(WSGIAHndler):
    
    def file_iterator(self, f, size=100000):
        while True:
            block = f.read(size)
            if not block:   break
            yield block

    # URL looks like this: ..../get/request_id/file_path
    def get(self, request, relpath, **args):
        if request.method != "GET":
            return Response(status="400 GET method expected")
        tid, path = relpath.split("/", 1)
        tid = int(rid)
        t = self.App.Storage.transaction(tid)
        if not t:
            return Response(status="404 not found")
        info = t.Info
        if info.Path != path:
            return Response(status="400 file path mismatch")
        with t.open() as f:
            file_iter = self.file_iterator(f)
            return Response(app_iter = file_iter)
        
    # URL looks like this: ..../put?rid=request_id
    def put(self, request, relpath, rid=None, **args):
        if request.method != "POST" and request.method != "PUT":
            return Response(status="400 POST method expected")
        tid = int(rid)
        t = self.App.Storage.transaction(tid)
        if not t:
            return Response(status="404 not found")
        info = t.Info
        if info.Path != path:
            return Response(status="400 file path mismatch")
        self.App.Storage.removeTransaction(t)
        with t.open() as f:
            while True:
                block = request.body_file.read(100000)
                if not block:   break
                f.write(block)
        return Response("OK")
        
        
        
        
