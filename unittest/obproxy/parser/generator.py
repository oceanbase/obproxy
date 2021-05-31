#!/bin/env python
#coding=utf-8
import argparse
import subprocess
import sys
import os

class MainHandler():
    def __init__(self, opt):
        self.opt = opt
        self.ofile = open(self.opt.filename, 'w+')

    def __del__(self):
        self.ofile.close()

    def check_file_type(self, filename):
        return filename.endswith('.sql') \
               or filename.endswith('.inc') \
               or filename.endswith('.test')

    # dfs travel
    def travel(self, path):
        if (self.opt.verbose):
            print path

        if os.path.isdir(path):
            for sub_path in os.listdir(path):
                self.travel(os.path.join(path, sub_path))
        elif os.path.isfile(path) and self.check_file_type(os.path.basename(path)):
            for l in file(path):
                if l.strip().startswith('--') or l.strip().startswith('#') or l.strip().startswith('connection') \
                   or l.strip().startswith('while') or l.strip().startswith('{') or l.strip().startswith('}') \
                   or l.strip().startswith('let') or l.strip().startswith('source') or l.strip().startswith('inc') \
                   or l.strip().startswith('dec') \
                   or len(l.strip()) == 0:
                    pass
                else:
                    self.ofile.write(l)
        else:
            pass

    def run_normal(self):
        if os.path.exists(self.opt.dirname):
            self.travel(self.opt.dirname)
        else:
            print 'invalid dir %s' % (self.opt.dirname)

    def run(self):
        if (self.opt.mode == 'normal'):
            self.run_normal()
        else:
            print 'invalid mode'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dirname", help="dir to get all", action='store', dest='dirname', default="../../../tools/deploy/mysql_test")
    parser.add_argument("-m", "--mode", help="get sql mode", action='store', dest='mode', default="normal")
    parser.add_argument("-f", "--filename", help="output file", action='store', dest='filename', default="mysqltest.sql")
    parser.add_argument("-v", "--verbose", help="is verbose", action='store_true', dest='verbose', default=False)
    args = parser.parse_args()

    MainHandler(args).run()
