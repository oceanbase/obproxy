#!/bin/env python
#coding=utf-8
import argparse
import subprocess
import sys
import os

PARSER_CHECKER_NAME = 'obproxy_parser_checker'

def run_cmd(cmd, need_print=True, cwd=None):
  # print cmd
  res = ''
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd)
  while True:
      line = p.stdout.readline()
      res += line
      if line:
          if (need_print):
              print line.strip()
              sys.stdout.flush()
      else:
        break
  p.wait()
  return "run_cmd failed" if p.returncode != 0 else res

def print_info(info_str):
    print '\033[32m[=== %-25.25s ===]\033[0m' % info_str

class MainHandler():
    def __init__(self, opt):
        self.opt = opt

    def gen_sql(self):
        # gen test file if not exists
        if (not os.path.isfile(self.opt.test_case)):
            if (os.path.isfile(self.opt.generator)):
                run_cmd("./%s -f %s" % (self.opt.generator, self.opt.test_case))
            else:
                print 'generator not set, use -g generator.py to set generator'

    def gen_base_result(self):
        # gen expect file if not exists
        result_file_name = self.opt.test_case.replace('.sql', '.result').replace('.test', '.result')
        if (not os.path.isfile(result_file_name)):
            if (os.path.isfile(self.opt.base_parser)):
                run_cmd("./%s -f %s -r %s" % (self.opt.base_parser, self.opt.test_case, result_file_name))
            else:
                url = 'http://ip:port/obproxy_build/%s' % self.opt.base_parser
                print 'get base parser from %s' % url
                run_cmd("wget %s -O %s &>/dev/null && chmod a+x %s" % (url, self.opt.base_parser, self.opt.base_parser))
                if (os.path.isfile(self.opt.base_parser)):
                    run_cmd("./%s -f %s -r %s" % (self.opt.base_parser, self.opt.test_case, result_file_name))
                else:
                    print 'fail to get base_parser'

    def gen_target_result(self):
        # gen expect file if not exists
        result_file_name = self.opt.test_case.replace('.sql', '.tmp').replace('.test', '.tmp')
        if (os.path.isfile(self.opt.target_parser)):
            run_cmd("./%s -f %s -r %s" % (self.opt.target_parser, self.opt.test_case, result_file_name))
        else:
            run_cmd("cd .. && make %s -j10 && cp %s parser/%s" % (PARSER_CHECKER_NAME, PARSER_CHECKER_NAME, self.opt.target_parser))
            run_cmd("./%s -f %s -r %s" % (self.opt.target_parser, self.opt.test_case, result_file_name))

    def run_comparer(self):
        base_result = self.opt.test_case.replace('.sql', '.result').replace('.test', '.result')
        target_result = self.opt.test_case.replace('.sql', '.tmp').replace('.test', '.tmp')
        run_cmd("./%s --expect=%s --result=%s" % (self.opt.comparer, base_result, target_result))

    def run(self):
        print_info('Generator test file')
        self.gen_sql()
        print_info('Generator base result')
        self.gen_base_result()
        print_info('Generator target result')
        self.gen_target_result()
        print_info('Run comparer')
        self.run_comparer()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--generator", help="generator", action='store', dest='generator', default="generator.py")
    parser.add_argument("-c", "--comparer", help="comparer", action='store', dest='comparer', default="comparer.py")
    parser.add_argument("-t", "--test-case", help="test case", action='store', dest='test_case', default="mysqltest.sql")
    parser.add_argument("-bp", "--base-parser", help="base parser, gen expect file", action='store', dest='base_parser', default="base_parser")
    parser.add_argument("-tp", "--target-parser", help="target parser, gen result file", action='store', dest='target_parser', default="target_parser")
    args = parser.parse_args()

    MainHandler(args).run()
