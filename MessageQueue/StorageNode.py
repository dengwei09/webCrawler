import threading
import mmap
import os
import platform
import time
#maximum size for a node
NODE_FILE_SIZE = 5*1024*1024
class NodeExistsError(Exception): pass
class NodeNotSafetyShutdown(Exception): pass
class NodeNoEnoughSpace(Exception): pass

class StorageNode(object):
    def __init__(self,dir_, size = NODE_FILE_SIZE):
        #use mutex for threads(get access to public resources)
        if not os.path.exists(dir_):
            raise Exception("The directory is not exist")
        
        self.lock = threading.Lock()
        self.lockFile = os.path.join(dir_,'lock')
        with self.lock:
            if os.path.isfile(self.lockFile):
                raise NodeExistsError('This directory has been used by other node')
            else:
                open(self.lockFile,'w').close()    
        
        #initialization
        self.dir_ = dir_
        self.size = 0
        self.NODE_FILE_SIZE = size
        
        self.oldFiles = []
        self.newFiles = []
        self.fileHandles = {}
        self.mapHandles = {}
        
        self.stop = False
        
        self.startInitialize()
  
    #remove path from fileHandles
    def _remove_file_handles(self,path):
        if path in self.fileHandles:
            self.fileHandles[path].close()
            del self.fileHandles[path]
                 
    #add path to fileHandles
    def _add_file_handles(self,path):
        if path not in self.fileHandles:
            fp = open(path,"a+")
            self.fileHandles[path] = fp
            return fp
        else:
            return None      
    
    #remove map file from mapHandles
    def _remove_map_handles(self,path):
        if path in self.mapHandles:
            self.mapHandles[path].close()
            del self.mapHandles[path]
    
    #add map file to mapHandles( cannot use when the file is empty)
    def _add_map_handles(self,path):
        if path not in self.mapHandles:
            mp = mmap.mmap(self.fileHandles[path].fileno(), self.NODE_FILE_SIZE)
            self.mapHandles[path] = mp
            return mp
        else:
            return None
    
    #write the object to the file
    #write in this way in order to use mmap object
    #obj at here is string object
    def _write_obj(self,obj,fp):
        if platform.system() == 'Windows':
            fp.write(obj)
        else:
            restLength = self.NODE_FILE_SIZE - len(obj)
            fp.write(obj+'\x00'*restLength)
    
    def startInitialize(self):
        #check the files in directory(lock file or .log file)
        files = os.listdir(self.dir_)
        for fl in files:
            if fl == 'lock':
                continue
            else:
                tmpFileLocation = os.path.join(self.dir_,fl)
                if not os.path.isfile(tmpFileLocation) or not fl.endswith('.log'):
                    raise NodeNotSafetyShutdown('the node has not been safely shutdown last time')
                
                #put all old .log file into list 'oldFiles'
                self.oldFiles.append(tmpFileLocation)
                
        if len(self.oldFiles)>0:
            #copy the names of file from 'oldFiles' into 'newFiles' without .log(1.log;2.log;3.log) --> (1;2;3)
            self.oldFiles = sorted(self.oldFiles, key = lambda fl: str((os.path.split(fl)[1]).rsplit('.',1)[0]))
            self.newFiles = [fl.rsplit('.',1)[0] for fl in self.oldFiles ]
            
            #copy the content from old file into new file
            for (old, new) in zip(self.oldFiles,self.newFiles):
                with open(old) as old_fp:
                    tmpContext = old_fp.read()
                    new_fp = open(new,'w+')
                    self._write_obj(tmpContext, new_fp)
                    #have to close it and then reopen it, otherwise, error will happen when use mmap
                    new_fp.close()
                    
                    #add new path ot dict 'fileHandles'
                    #add new path ot dict 'mapHandles' 
                    #sequence is important
                    self._add_file_handles(new)
                    self._add_map_handles(new)    

    
    #merge and combine the files to put into 5M files
    #can be optimized in the future
    ##################################################
    def merge(self):
        #stop put and get
        self.stop = True
        
        #use to merge 
        #input: [('1',5),('2',5),('3',5),('4',4),('5',2),('6',8)], 10
        #output:[['6', '5'], ['4', '3'], ['2', '1']]
        def pack(pair,_NODE_FILE_SIZE):
            names = [item[0] for item in pair]
            sizes = [item[1] for item in pair]

            nameCom = []
            tmpNameCom = []
            packSize = 0
            length = len(sizes)
            for _ in range(length):
                tmpSize = sizes.pop()
                tmpName = names.pop()
                
                if (packSize+tmpSize) > _NODE_FILE_SIZE:
                    nameCom.append(tmpNameCom)
                    tmpNameCom = [tmpName]
                    packSize = tmpSize
                else:
                    tmpNameCom.append(tmpName)
                    packSize += tmpSize
            
            nameCom.append(tmpNameCom)
            
            return nameCom
        
        pair = [] #use as input for pack function
        for (fl,mp) in self.mapHandles.items():
            pos = mp.rfind('\n') + 1
            pair.append((fl,pos))
          
        mergeSet = pack(pair,self.NODE_FILE_SIZE)
    
        #information storage after merge
        newFilesTmp = []
        
        for files in mergeSet:
            mpList = []
            for fl in files:
                mpList.append(self.mapHandles[fl])
        
            #create new file
            path = os.path.join(self.dir_,str(time.time()))
            
            #make sure the name is unique in newfilestmp and existing files. (avoid to have the same file name with existing file)
            path = self._uniqueflName(path,self.newFiles)
            path = self._uniqueflName(path,newFilesTmp)

            newFilesTmp.append(path)
            
            fp = open(path,'w+')
            context = ''
            
            #mp[0:3] means mp[0], mp[1] and mp[2]. not include mp[3]
            for mp in mpList:
                pos = mp.rfind('\n')+1
                context += mp[:pos]
            
            self._write_obj(context, fp)
            fp.close()           

        #reinitialize after merge, mutex has been used      
        with self.lock:
            #remove the old files for merge
            for path in self.newFiles:               
                #the sequence for removal is important for map and file
                self. _remove_map_handles(path)
                self._remove_file_handles(path)
                os.remove(path)
            
            #add the new file points to maphandle and filehandle
            #sequence is important
            self.newFiles = newFilesTmp    
            for path in self.newFiles:
                self._add_file_handles(path)
                self._add_map_handles(path)
    
    #separate the list of obj via '\n'    
    def _objStr(self,obj): 
        if isinstance(obj,(list,tuple)):
            obj = '\n'.join(obj) + '\n'            
        else:
            obj = str(obj) + '\n'
        return obj
    
    #creates the unique file name
    def _uniqueflName(self,flName,flNameList):
        i = 1
        while flName in flNameList:
            flName = flName + '_' + str(i)
        return flName
              
    def put(self,obj):
        #check stopped tag
        if self.stop :
            return False     
        #return obj(which converted to string object)
        obj = self._objStr(obj)
        #check the size of obj
        if len(obj)>self.NODE_FILE_SIZE:
            raise NodeNoEnoughSpace('the obj is larger than the node size') 
                   
        #put obj into file via the file mapping from mapHandles
        createNewFile = True
        for fileNm in self.newFiles:
            with self.lock:
                mp = self.mapHandles[fileNm]
                size = mp.rfind('\n') + 1 
                newSize = size + len(obj)
                if newSize > self.NODE_FILE_SIZE:
                    continue
                else:
                    createNewFile = False
                    mp[:newSize] = mp[:size] + obj
                    mp.flush()
                    break
                    
        #there is no old file available, therefore to create a new file to hold the content
        with self.lock:
            if createNewFile:
                tmpName = str(time.time())
                
                #append file name to newFiles
                tmpName = os.path.join(self.dir_,tmpName)
                tmpName = self._uniqueflName(tmpName,self.newFiles)
                self.newFiles.append(tmpName)
                
                fp = open(tmpName,'w+')
                
                #have to be close at first, then add to filehandle and maphandle
                self._write_obj(obj,fp)
                fp.close()
                self._add_file_handles(tmpName)
                self._add_map_handles(tmpName)
        return True
                        
    def get(self):
        # check stopped tag
        if self.stop:
            return None
        # get a obj via find'\n', and extract obj, fill the empty via '/x00' at the end of mmap.mmap
        for mp in self.mapHandles.values():
            pos = mp.find('\n')
            if pos == -1:
                continue
            obj = mp[:pos]#not include '\n'
            mp[:] = mp[pos+1:] + '\x00'*(pos+1)
            
            mp.flush()
            obj = obj.strip()
            if obj is '':
                pass
            else:
                return obj
                  
    def shutdown(self):
        try:            
            self.stop = True
            self.merge()
            #delete previous log files
            for fl in self.oldFiles:
                os.remove(fl)   
            
            #close the file points       
            for fl in self.newFiles:
                #the sequence for removal is important for map and file
                self. _remove_map_handles(fl)
                self._remove_file_handles(fl)                
            #rename the files with '.log'    
            for fl in self.newFiles:
                os.rename(fl,fl+'.log')  
        finally:
            with self.lock:    
                os.remove(self.lockFile)
    
    def remove(self):
        try:            
            self.stop = True
            #delete previous log files
            for fl in self.oldFiles:
                os.remove(fl)   
            
            #close the file points       
            for fl in self.newFiles:
                #the sequence for removal is important for map and file
                self. _remove_map_handles(fl)
                self._remove_file_handles(fl)                
            #rename the files with '.log'    
            for fl in self.newFiles:
                os.remove(fl)  
        finally:
            with self.lock:    
                os.remove(self.lockFile)
                
    def __enter__(self):
        return self
    
    def __exit__(self):
        self.shutdown()

    
if __name__ == "__main__":
    path = os.path.join(os.path.abspath('.'),'test')
    abc = StorageNode(path,10)   
    abc.put(['abc','aaaa'])
    abc.put(['a'])
    abc.put(['fabc'])
    abc.put(['ffff','cccc'])
    abc.put(['hhhh'])
    print abc.get()
    print abc.get()
    abc.shutdown()
