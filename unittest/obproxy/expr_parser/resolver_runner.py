#!/bin/env python
#coding=utf-8
import argparse
import subprocess
import sys
import os
import re

def run_cmd(cmd, need_print=True, cwd=None):
  # print cmd
  print cmd
  res = ''
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd)
  while True:
      line = p.stdout.readline()
      res += line
      if line:
          if (need_print):
              print line.strip('\n')
              sys.stdout.flush()
      else:
        break
  p.wait()
  return "run_cmd failed" if p.returncode != 0 else res

if __name__ == '__main__':
    run_cmd("./runner.py -t'test_expr_resolver.test' -r'test_expr_resolver.result' -e'-R' " + ' '.join(sys.argv[1:]))
