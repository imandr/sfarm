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

    def findFile(self, path, version, wait=5.0):
        sock = socket.socket(sock.AF_INET, socket.SOCK_DRGAM)
        dgram = self.packMessage("findFile", path, version)
        sock.sendto(dgram, self.BAddr)
        sock.settimeout(wait)
        try:    
            msg, addr = sock.recvfom(MAXMSG)
        except socket.timeout:
            return None
        (reply, url), args = self.unpackMessage(msg)
        assert reply == "findFile"
        return url
        
    def acceptFile(self, path, version, replicas, wait=5.0):
        sock = socket.socket(sock.AF_INET, socket.SOCK_DRGAM)
        dgram = self.packMessage("acceptFile", path, version, replicas)
        sock.sendto(dgram, self.BAddr)
        sock.settimeout(wait)
        try:    
            msg, addr = sock.recvfom(MAXMSG)
        except socket.timeout:
            return None
        (reply, url), args = self.unpackMessage(msg)
        assert reply == "acceptFile"
        return url
        
    def removeVersions(self, path, max_version):
        sock = socket.socket(sock.AF_INET, socket.SOCK_DRGAM)
        dgram = self.packMessage("removeVersions", path, max_version)
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
            if msg == "findFile":
                reply = self.findFile(*msg[:1], **args)
            elif msg == "acceptFile":
                reply = self.acceptFile(*msg[:1], **args)
            elif msg == "removeVersions":
                reply = self.removeVersions(*msg[:1], **args)
            if reply is not None:
                if not isinstance(reply, tuple):    reply = (reply,)
                reply = (msg,) + reply
                sock.sendto(packMessage(*reply), addr)
                
    def findFile(self, path, version):
        filedesc = self.Storage.findFile(path)
        if filedesc and filedesc.Version == version:
            r = self.RequestKeeper.downloadRequest(path, version)
            return r.URL
        
    def acceptFile(self, path, version):
        filedesc = self.Storage.findFile(path)
        if filedesc is None or version != filedesc.Version:
            if fildedesc is not None:
                self.Storage.removeFile(path, filedesc.Version)
            r = self.RequestKeeper.uploadRequest(path, version)
            return r.URL
    
    def removeVersions(self, path, max_version):
        filedesc = self.Storage.findFile(path)
        if filedesc is not None and filedesc.Version <= max_version:
            self.Storage.removeFile(path, filedesc.Version)
            return "OK", filedesc.Version
        else:
            return "OK"
        
            
        
        
        
        
                
              
        
        
        
        
        
