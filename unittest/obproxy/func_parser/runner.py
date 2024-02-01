#!/bin/env python
#coding=utf-8
#__author__ = 'feizhi.cfz'
import argparse
import subprocess
import sys

def run_cmd(cmd, need_print=True, cwd=None):
  # print cmd
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

def print_info(info_str):
    print '\033[32m[=== %-10.10s ===]\033[0m' % info_str

def print_error(error_str):
    print '\033[31m[=== %-10.10s ===]\033[0m' % error_str

class MainHandler():
    def __init__(self, opt):
        self.opt = opt
        self.tmp_file = self.opt.test_file.replace('.test', '.tmp')

    def run_file(self):
        fp = open(self.opt.test_file, 'r')
        line = fp.readline()
        while line:
          line = line.strip('\n')
          if not line.startswith("//") :
            self.run_checker(line)
          line = fp.readline()


    def run_checker(self, input_str):
      run_cmd("cd .. && cp %s func_parser/%s && cd func_parser" % (self.opt.checker, self.opt.checker))
      run_cmd("./%s -s \"%s\" -r \'%s\' -m %s" % (self.opt.checker, input_str, self.opt.result_file, self.opt.mode))

    def run(self):
        self.run_file()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--checker", action='store', dest='checker', default='ob_func_expr_parser_checker')
    parser.add_argument("-t", "--test-file", help="test case", action='store', dest='test_file', default="test_expr.test")
    parser.add_argument("-r", "--result-file", help="test case", action='store', dest='result_file', default="test_expr.result")
    parser.add_argument("-m", "--mode", help="test case", action='store', dest='mode', default="mysql")
    args = parser.parse_args()

    MainHandler(args).run()


