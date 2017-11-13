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

    def getFile(self, path, version, wait=5.0):
        sock = socket.socket(sock.AF_INET, socket.SOCK_DRGAM)
        dgram = self.packMessage("get", path, version)
        sock.sendto(dgram, self.BAddr)
        sock.settimeout(wait)
        try:    
            msg, addr = sock.recvfom(MAXMSG)
        except socket.timeout:
            return None
        (reply, url), args = self.unpackMessage(msg)
        assert reply == "get"
        return url
        
    def putFile(self, path, version, size, replicas, wait=5.0):
        sock = socket.socket(sock.AF_INET, socket.SOCK_DRGAM)
        dgram = self.packMessage("put", path, version, size, replicas)
        sock.sendto(dgram, self.BAddr)
        sock.settimeout(wait)
        try:    
            msg, addr = sock.recvfom(MAXMSG)
        except socket.timeout:
            return None
        (reply, url), args = self.unpackMessage(msg)
        assert reply == "put"
        return url
        
    def removeVersions(self, path, max_version):
        sock = socket.socket(sock.AF_INET, socket.SOCK_DRGAM)
        dgram = self.packMessage("rm", path, max_version)
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
                reply = self.removeVersions(*msg[:1], **args)
            if reply is not None:
                if not isinstance(reply, tuple):    reply = (reply,)
                reply = (msg,) + reply
                sock.sendto(packMessage(*reply), addr)
                
    def getFile(self, path, version):
        filedesc = self.Storage.findFile(path)
        if filedesc and filedesc.Version == version:
            t = self.Storage.getTransaction(path, version)
            r = self.RequestKeeper.downloadRequest(t)
            return r.URL
        
    def putFile(self, path, version, size, replicas):
        filedesc = self.Storage.findFile(path)
        if filedesc is None or version != filedesc.Version:
            if fildedesc is not None:
                self.Storage.delFile(path, filedesc.Version)
            t = self.Storage.putTransaction(path, version, size)
            if t is not None:
                r = self.RequestKeeper.uploadRequest(t, replicas)
            return r.URL
    
    def removeVersions(self, path, max_version):
        filedesc = self.Storage.findFile(path)
        if filedesc is not None and filedesc.Version <= max_version:
            self.Storage.removeFile(path, filedesc.Version)
            return "OK", filedesc.Version
        else:
            return "OK", None
        
class CellServerApp(WSGIApp):
    
    def __init__(self, request, handler_class, request_keeper, storage):
        WSGIApp.__init__(request, handler_class)
        self.RequestKeeper = request_keeper
        self.Storage = storage

    # URL looks like this: ..../get/request_id/file_path
    def get(self, request, relpath, **args):
        rid, path = relpath.split("/", 1)
        rid = int(rid)
        r = self.RequestKeeper.request(rid)
        if not r:
            return Response(status="404 not found")
        if r.Path != path:
            self.RequestKeeper.removeRequest(r.ID)
            return Response(status="400 file path mismatch")
        file_iter = self.Storage.fileIterator(path, r.Version)
        if file_iter is None:
            return Response(status="404 not found")
        return Response(app_iter = file_iter)
        
    # URL looks like this: ..../put?rid=request_id
    def put(self, request, relpath, rid=None, **args):
        if request.method != "POST":
            return Response(status="400 POST method expected")
        rid = int(rid)
        r = self.RequestKeeper.request(rid)
        if not r:
            return Response(status="404 not found")
        try:    
            with self.Storage.storeTransaction(r.Path, r.Version) as f:
                
            self.Storage.storeFile(r.Path, r.Version, request.body_file)
        except:
            return Response(status="500 storage error")
        return Response("OK")
        
        
        
        