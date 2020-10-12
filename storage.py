import rpyc
import uuid
import os
import exiftool

from rpyc.utils.server import ThreadedServer

DATA_DIR="/mnt/c/users/user/desktop/ds/pydfs/storage/"

class StorageService(rpyc.Service):
  class exposed_storage():

    def exposed_init(self):
      if os.listdir(DATA_DIR):
        os.system("rm -r " + DATA_DIR + "*")

    def exposed_put(self,block_uuid,data,minions):
      with open(DATA_DIR+str(block_uuid),'w') as f:
        f.write(data)
      if len(minions)>0:
        self.forward(block_uuid,data,minions)

    def exposed_get(self,block_uuid):
      block_addr=DATA_DIR+str(block_uuid)
      if not os.path.isfile(block_addr):
        return None
      with open(block_addr) as f:
        return f.read()   
        
    def exposed_info(self,block_uuid):
      block_addr = DATA_DIR+str(block_uuid)
      if not os.path.isfile(block_addr):
        return None
      with exiftool.ExifTool() as et:
        metadata = et.get_metadata_batch([block_addr])
      return metadata[0]
        
    def exposed_rmv(self,block_uuid):
      block_addr=DATA_DIR+str(block_uuid)
      if not os.path.isfile(block_addr):
        return
      os.remove(block_addr)

    def forward(self,block_uuid,data,minions):
      print ("8888: forwarding to:")
      print (block_uuid, minions)
      minion=minions[0]
      minions=minions[1:]
      host,port=minion

      con=rpyc.connect(host,port=port)
      minion = con.root.Minion()
      minion.put(block_uuid,data,minions)

    def delete_block(self,uuid):
      pass

if __name__ == "__main__":
  if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
  t = ThreadedServer(StorageService, port = 8888)
  t.start()