import rpyc
import uuid
import os

from rpyc.utils.server import ThreadedServer

DATA_DIR="storage"
root="/mnt/c/users/user/desktop/ds/pydfs/"

class StorageService(rpyc.Service):
  class exposed_storage():
  
    def exposed_init(self):
      os.chdir(root)
      os.chdir(DATA_DIR)
      if os.listdir(root+DATA_DIR):
        os.system('rm -r *')
      return os.getcwd()

    def exposed_mkdir(self,dir):
      os.system('mkdir '+dir)
    
    def exposed_cddir(self,dir):
      if(os.path.isdir(dir)):
        os.chdir(dir)
      return os.getcwd()
    
    def exposed_rmdir(self,dir):
      os.system('rmdir '+ dir)
    
    def exposed_ls(self):
      return os.listdir()

if __name__ == "__main__":
  if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
  t = ThreadedServer(StorageService, port = 8888)
  t.start()