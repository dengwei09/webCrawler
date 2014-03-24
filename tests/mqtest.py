from MessageQueue.MessageQueue import MessageQueue
from core.rpc import MinerThreadedXMLRPCServer
from MessageQueue.HashRing import HashRing
import tempfile
import threading
import random

if __name__ == "__main__":
    ports = (11111, 11211, 11311)
    nodes = ['localhost:%s'%port for port in ports]
    dirs = [tempfile.mkdtemp() for _ in range(len(ports))]
    
    print dirs
    
    size = len(ports)
    
    rpc_server=[]
    mq=[]
    for i in range(size):
        rpc_server.append(MinerThreadedXMLRPCServer(('localhost', ports[i])))
        hashring = HashRing(nodes)
        
        mq.append(MessageQueue(nodes[:], nodes[i], rpc_server[i], hashring, dirs[i]))
        
        
        thd = threading.Thread(target=rpc_server[i].serve_forever)
        thd.setDaemon(True)
        thd.start()
    
    print 'success'
    mqTest = mq[0]
    data = [str(random.randint(10000, 50000)) for _ in range(300)]
          
    mqTest.put(data)
    print 'done'
    gets = []
    
    while True:
        get = mqTest.get()
        print get
        if get is None:
            break
        gets.append(get)  
             
    print gets
    

