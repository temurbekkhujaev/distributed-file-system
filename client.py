import rpyc
import sys
import os

def main(args):
  con=rpyc.connect("localhost",port=2131)
  master=con.root.Master()
  
  if args[0] == "init":
    master.init()
  elif args[0] == "mkdir":
    master.mkdir(args[1])
  elif args[0] == "rmdir":
    master.rmdir(args[1])
  elif args[0] == "ls":
    print(master.ls())
  elif args[0] == "cd":
    master.cd(args[1])
  elif args[0] == "pwd":
    master.pwd()


if __name__ == "__main__":
  main(sys.argv[1:])
