#!/bin/env python
#coding=utf-8
import argparse
import subprocess
import sys
import os
import re

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
        data = fp.read();
        run_cmd("rm -rf %s" % self.tmp_file);
        all_match = re.findall("schema\s*=(.*?)\nsql\s*=(.*?);", data)
        for match in all_match:
            self.run_checker(match[1], match[0])

    def run_checker(self, input_str, extra_str):
        if (os.path.isfile(self.opt.checker)):
            run_cmd("./%s -s \"%s\" -e '%s' -r %s %s" % (self.opt.checker, input_str, extra_str, self.tmp_file, self.opt.extra_option))
        else:
            run_cmd("cd .. && make %s -j10 && cp %s expr_parser/%s" % (self.opt.checker, self.opt.checker, self.opt.checker))
            run_cmd("./%s -s \"%s\" -e '%s' -r %s %s" % (self.opt.checker, input_str, extra_str, self.tmp_file, self.opt.extra_option))

    def run_comparer(self):
        result = run_cmd("diff %s %s" % (self.tmp_file, self.opt.result_file))
        if (result.strip() == ""):
            print_info('SUCCESS')
            run_cmd("rm -rf %s" % (self.tmp_file))
        else:
            print_error("FAIL")
            print result

    def run(self):
        self.run_file()
        if (not self.opt.record):
            self.run_comparer()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--checker", action='store', dest='checker', default='ob_expr_parser_checker')
    parser.add_argument("-t", "--test-file", help="test case", action='store', dest='test_file', default="test_expr.test")
    parser.add_argument("-r", "--result-file", help="test case", action='store', dest='result_file', default="test_expr.result")
    parser.add_argument("-R", "--record", help="record result, do not compare", action='store_true', dest='record', default=False)
    parser.add_argument("-e", "--extra_option", help="extra option(-S, -R, -n)", dest='extra_option', default="")
    args = parser.parse_args()

    MainHandler(args).run()
