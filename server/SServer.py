from threads import Primitive
from wsgi_py import WSGIApp, WSGIHandler, Application
from VDatabase import VDatabase
from Cell import CellBroadcastRequest

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
        filepath = relpath
        fd = VDatabase.findFile(filepath)
        if not fd:
            return Response(status="404 Not found")
        cb = self.App.broadcastRequest()
        url = cb.findFile(filepath, fd.Version)
        if not url:
            return Response(status="408 Time-out")
        self.redirect(url)
        
    def put(self, req, relpath, replicas = 1, **args):
        if req.method != "POST":
            return Response(status="400 Bad request (method must be POST)")
        replicas = int(replicas)
        filepath = relpath
        fd = VDatabase.createFile(filepath)
        if not fd:
            return Response(status="404 Can not create file version")
        cb = self.App.broadcastRequest()
        url = cb.acceptFile(filepath, fd.Version, replicas)
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
        filepath = relpath
        fd = VDatabase.findFile(filepath)
        if not fd:
            return Response(status="404 Not found")
        VDatabase.removeFile(filepath)
        self.App.broadcastRequest().removeVersions(filepath, fd.Version)
        return Response("OK")
            
application = Application(WSGIApp, WSGIhandler)
                
