import rpyc
import uuid
import threading 
import math
import random
import configparser
import signal
import pickle
import sys
import os

from rpyc.utils.server import ThreadedServer


def set_conf():
  conf=configparser.ConfigParser()
  conf.readfp(open('dfs.conf'))
  MasterService.exposed_Master.block_size = int(conf.get('master','block_size'))
  MasterService.exposed_Master.replication_factor = int(conf.get('master','replication_factor'))
  storages = conf.get('master','storages').split(',')
  for m in storages:
    id,host,port=m.split(":")
    MasterService.exposed_Master.storages[id]=(host,port)

class MasterService(rpyc.Service):
  class exposed_Master():
    file_table = {}
    storages = {}
    cur_dir = None
    block_size = 0
    replication_factor = 0
	
    #DIRECTORY OPERATIONs
    def exposed_init(self):
      self.__class__.file_table = {}
      for storage in self.__class__.storages:
        host,port = self.__class__.storages[storage]
        con = rpyc.connect(host,port=port)
        storage = con.root.storage()
        self.__class__.cur_dir = storage.init()

    def exposed_mkdir(self,dir):
      for storage in self.__class__.storages:
        host,port = self.__class__.storages[storage]
        con=rpyc.connect(host,port=port)
        storage = con.root.storage()
        storage.mkdir(dir)
    
    def exposed_rmdir(self,dir):
      for storage in self.__class__.storages:
        host,port = self.__class__.storages[storage]
        con=rpyc.connect(host,port=port)
        storage = con.root.storage()
        storage.rmdir(dir)
    
    def exposed_cd(self,dir):
      for storage in self.__class__.storages:
        host,port = self.__class__.storages[storage]
        con=rpyc.connect(host,port=port)
        storage = con.root.storage()
        self.cur_dir = storage.cddir(dir)

    def exposed_ls(self):
      for storage in self.__class__.storages:
        storage = self.__class__.storages[storage]
        host,port = storage
        con=rpyc.connect(host,port=port)
        storage = con.root.storage()
        return storage.ls()

    def exposed_pwd(self):
      print(self.__class__.cur_dir)

    #END


if __name__ == "__main__":
  set_conf()
  signal.signal(signal.SIGINT,int_handler)
  t = ThreadedServer(MasterService, port = 2131)
  t.start()