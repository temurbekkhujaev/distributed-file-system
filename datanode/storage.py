import rpyc
import uuid
import os

from rpyc.utils.server import ThreadedServer

DATA_DIR="storage/"

class StorageService(rpyc.Service):
  class exposed_storage():

    def exposed_init(self):
      if os.listdir(DATA_DIR):
        os.system("rm -r " + DATA_DIR + "*")

    def exposed_put(self,block_uuid,data,storages):
      with open(DATA_DIR+str(block_uuid),'w') as f:
        f.write(data)
      if len(storages)>0:
        self.forward(block_uuid,data,storages)

    def exposed_get(self,block_uuid):
      block_addr=DATA_DIR+str(block_uuid)
      if not os.path.isfile(block_addr):
        return None
      with open(block_addr) as f:
        return f.read()   
        
    def exposed_rmv(self,block_uuid):
      block_addr=DATA_DIR+str(block_uuid)
      if not os.path.isfile(block_addr):
        return
      os.remove(block_addr)

    def forward(self,block_uuid,data,storages):
      print ("8888: forwarding to:")
      print (block_uuid, storages)
      storage=storages[0]
      storages=storages[1:]
      host,port=storage

      con=rpyc.connect(host,port=port)
      storage = con.root.storage()
      storage.put(block_uuid,data,storages)


if __name__ == "__main__":
  if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
  t = ThreadedServer(StorageService, port = 8888)
  t.start()
