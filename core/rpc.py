from SocketServer import ThreadingMixIn
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import xmlrpclib
import os
import socket

RETRY_TIMES = 10

def test_plus_one(num):
    return num + 1

class MinerThreadedXMLRPCServer(ThreadingMixIn, \
                        SimpleXMLRPCServer): 
    def __init__(self, *args, **kwargs):        
        SimpleXMLRPCServer.__init__(self, *args, **kwargs)
        
    def serve_forever(self):
        self.quit = False
        while not self.quit:
            self.handle_request()
            
    def kill(self):
        self.quit = True

def clientCall(server, func_name, *args, **kwargs):
    serv = xmlrpclib.ServerProxy('http://%s' % server)
    ignore = kwargs.get('ignore', False)
    #we can ignore the call
    if not ignore:
        err = None
        retry_times = 0
        while retry_times <= RETRY_TIMES:
            try:
                return getattr(serv, func_name)(*args)
            except socket.error, e:
                retry_times += 1
                err = e
        raise err
    else:
        try:
            return getattr(serv, func_name)(*args)
        except socket.error:
            pass       

if __name__ == "__main__":
    
    # Create server
    server = MinerThreadedXMLRPCServer(('localhost', 8080))
  
    server.register_function(test_plus_one, 'add')
    server.register_function(server.kill,'kill')
    
    # Run the server's main loop
    server.serve_forever()
    print 'done'
                 