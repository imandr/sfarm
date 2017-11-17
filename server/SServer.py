from threads import Primitive
from wsgi_py import WSGIApp, WSGIHandler, Application
from VDatabase import VDatabase
from Cell import CellBroadcastRequest
from VFSInfo import VFSCanonicPath

class SServerApp(WSGIApp):

    def __init__(self, config):
        self.Config = config
        
    def broadcastRequest(self):
        return CellBroadcastRequest(self.Config.BroadcastAddress)
    
    
class SServerHandler(WSGIhandler):

    def __init__(self, ...):
        pass
        
    def get(self, req, relpath, **args):
        if req.method != "GET":
            return Response(status="400 Bad request (method must be GET)")
        lpath = VFSCanonicPath(relpath)
        info = VDatabase.findFile(lpath)
        if not info:
            return Response(status="404 Not found")
        cb = self.App.broadcastRequest()
        url = cb.getFile(info)
        if not url:
            return Response(status="408 Time-out")
        self.redirect(url)
        
    def put(self, req, relpath, replicas = 1, size = None, **args):
        if req.method != "POST":
            return Response(status="400 Bad request (method must be POST)")
        replicas = int(replicas)
        lpath = VFSCanonicPath(relpath)
        size = int(size)
        info = VDatabase.createNextVersion(lpath)
        if not info:
            return Response(status="404 Can not create file version")
        info,setActualSize = size
        cb = self.App.broadcastRequest()
        url = cb.putFile(info, replicas)
        if not url:
            return Response(status="408 Time-out")        
        return Response(url)
        
    def listToJSON(self, lst):
        yield "["
        first = True
        for item in lst:
            if not first:
                yield "," + item.toJSON()
            else:
                yield item.toJSON()
        yield "]"

    def list(self, req, relpath, **args):
        if req.method != "GET":
            return Response(status="400 Bad request (method must be GET)")
        path = relpath
        return Response(content_type = "text/json", app_iter = self.listToJSON(VDatabase.list(path)))
        
    def remove(self, req, relpath, **args):
        if req.method != "POST":
            return Response(status="400 Bad request (method must be POST)")
        lpath = VFSCanonicPath(relpath)
        info = VDatabase.findFile(lpath)
        if not info:
            return Response(status="404 Not found")
        VDatabase.removeFile(info)
        self.App.broadcastRequest().removeVersions(info, info.Version)
        return Response("OK")
            
application = Application(WSGIApp, WSGIhandler)
                
