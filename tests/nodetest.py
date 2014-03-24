import tempfile
import os
import random
import xmlrpclib

from MessageQueue.StorageNode import StorageNode
from core.rpc import MinerThreadedXMLRPCServer



dir_ = tempfile.mkdtemp()
node = StorageNode(dir_)
print dir_
print 'test1'   
rpcServer = MinerThreadedXMLRPCServer(('localhost', 8080))   
rpcServer.register_function(node.put, 'put')
rpcServer.register_function(node.get, 'get')
rpcServer.serve_forever()
