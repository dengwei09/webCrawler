import tempfile
import os
import random
import xmlrpclib

from MessageQueue.StorageNode import StorageNode
from core.rpc import MinerThreadedXMLRPCServer



dir_ = tempfile.mkdtemp()
print dir_
node = StorageNode(dir_)
node.put(['1'*10,'2'*10])
node.shutdown()

node = StorageNode(dir_)
node.put(['3'*10])
node.shutdown()