from StorageNode import StorageNode
import HashRing
import time
import os
from core.rpc import clientCall

class MessageQueue(object):
    def __init__(self,nodes,localNode = None, rpcServer = None, hashring = None, taskDirPath = None):
        # all nodes in the network
        self.nodes = nodes
        if localNode == None:
            from core.utils import get_ip_address
            self.localNode = get_ip_address()
        else:
            self.localNode =localNode
            
        if rpcServer == None:
            from core.rpc import MinerThreadedXMLRPCServer
            #################################################################
            #port is temporary, will be replaced by a function
            self.rpcServer = MinerThreadedXMLRPCServer((self.localNode, 8080))
        else:
            self.rpcServer = rpcServer

        if hashring == None:
            self.hashring = HashRing(self.nodes)
        else:
            self.hashring = hashring
        
        if taskDirPath == None:
            currPath = os.path.abspath(__file__)
            #get the path of data
            self.taskDirPath = os.path.join(os.path.dirname(os.path.dirname(currPath)),'data')          
        else:
            self.taskDirPath = taskDirPath
        
        if not os.path.isdir(self.taskDirPath):
            os.mkdir(self.taskDirPath)
                        
        self._initStore(self.taskDirPath)

    def _initStore(self,taskDirPath):
        self.localStore = StorageNode(taskDirPath)
        #register the put function
        self.rpcServer.register_function(self.localStore.put, 'put')
        self.rpcServer.register_function(self.localStore.get, 'get')
        
    def _put(self,node,objs):   
        if node == self.localNode:
            return self.localStore.put(objs)    
        else:
            #'put' function is the put from StorageNode class
            return clientCall(node,'put',objs)
    
    def _get(self):
        return self.localStore.get()
        
    def put(self,objs):
        if isinstance(objs,(list,tuple)):
            #via hash_ring, put task object into different storage nodes. 
            #taksGrps record the distribution of task objects
            taskGrps = {}
            for tmpNode in self.nodes:
                taskGrps[tmpNode] = []
            for obj in objs:
                if isinstance(obj,str):
                    tmpNode = self.hashring.get_node(obj)
                    taskGrps[tmpNode].append(obj)
                else:
                    ValueError("the task can only be string type in MessageQueue")
                             
            for (key, val) in taskGrps.items():
                self._put(key,val)
                        
        elif isinstance(objs,str):
            tmpNode = self.hashring.get_node(objs)
            self._put(tmpNode,objs)
            
        else:
            ValueError("the task can only be string type in MessageQueue")      
            
    def get(self):
        while True:
            obj = None
            obj = self._get()
            if obj != None:
                return obj
            else:
                time.sleep(20)
    
    def taskTransfer(self):
        taskGrps = {}
        for tmpNode in self.nodes:
            taskGrps[tmpNode] = []    
                
        while True:
            obj = self.get()
            if obj == None:
                break
            tmpNode = self.hashring.get_node(obj)
            taskGrps[tmpNode].append(obj)
            
        for (key,val) in taskGrps:
            clientCall(key,'put',val)
            
    #permanently remove this node from network            
    def removeNode(self):
        pass
        #######################################################################
        #1,let master known, master will tell other nodes that this node shutdown
        #2,master update nodes and ask this nodes to transfer all its tasks to other nodes(via def taskTransfer)
            
    
    def shutdown(self):
        #######################################################################
        #1,let master known, master will tell other nodes that this node shutdown
        self.localStore.shutdown()        
       
    def __enter__(self):
        return self
    
    def __exit__(self):
        self.shutdown()