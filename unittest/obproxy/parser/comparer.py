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
        print line.strip()
        sys.stdout.flush()
    else:
      break
  p.wait()
  return "run_cmd failed" if p.returncode != 0 else res

SQL_PREFIX =    'SQL   : '
RESULT_PREFIX = 'RESULT: '
SQL_PREFIX_LEN = len(SQL_PREFIX)
RESULT_PREFIX_LEN = len(RESULT_PREFIX)
FAILED_SQL_FILE = 'failed.sql'

class ResultReader():
    def __init__(self, filename):
        self.f = open(filename, 'r')

    def get_next_row(self):
        sql_line = ""
        result_line = ""
        while True:
            line = self.f.readline()
            if (len(line) == 0):
                break
            if (line.startswith(SQL_PREFIX)):
                sql_line = sql_line + line[SQL_PREFIX_LEN:]
            elif (line.startswith(RESULT_PREFIX)):
                result_line = result_line + line[RESULT_PREFIX_LEN:]
                break
            else:
                sql_line = sql_line + '\n' + line
        result = {}
        for kv in result_line.split(','):
            m = re.split(':', kv.strip())
            if m and len(m) >= 2:
                result[m[0]] = m[1].strip('"').strip()
        return (sql_line, result)

class MainHandler():
    def __init__(self, opt):
        self.opt = opt
        self.failed_sql = []

    def run(self):
        sql_count = 0
        base_reader = ResultReader(self.opt.expect)
        target_reader = ResultReader(self.opt.result)
        while True:
            sql_count = sql_count + 1
            (base_sql, base_result) = base_reader.get_next_row()
            (target_sql, target_result) = target_reader.get_next_row()
            if (base_sql == "" or target_sql == ""):
                break;
            else:
                check_pass = True
                if (self.opt.mode == 'normal'):
                    check_pass = self.check_normal(base_sql, base_result, target_result)
                else:
                    print 'invalid parse mode'
                    exit(1)

                if (check_pass):
                    if (sql_count % 10000 == 0):
                        print 'finish %d result comparer' % sql_count
                        sys.stdout.flush()
                else:
                    print '\033[31m [FAILED]\033[0m'
                    print base_sql
                    print 'EXPECT RESULT: ' , base_result
                    print 'REJECT RESULT: ' , target_result
                    self.failed_sql.append(base_sql)

        if (len(self.failed_sql) > 0):
            f = open(FAILED_SQL_FILE, 'w+')
            for sql in self.failed_sql:
                f.write(sql + '\n')
            print 'failed sql saved in %s' % FAILED_SQL_FILE
        else:
            print '\033[32m [SUCCESS] \033[0m'

    def is_dml_stmt(self, stmt_type):
        return stmt_type in ['T_SELECT', 'T_DELETE', 'T_REPLACE', 'T_UPDATE', 'T_INSERT']

    def is_common_stmt(self, stmt_type):
        return stmt_type in ['T_INVALID', 'T_OTHERS']

    def check_normal(self, base_sql, base_result, target_result):
        # filter some sql
        orig_sql = base_sql.strip().lower()
        if (orig_sql.startswith('show') or orig_sql.startswith('kill') or orig_sql.startswith('alter')):
            return True
        elif (not base_result.has_key('stmt_type') or not target_result.has_key('stmt_type')):
            # tmp code
            return True
        else:
            base_result['stmt_type'] = base_result['stmt_type'].replace('OBPROXY_T', 'T')
            target_result['stmt_type'] = target_result['stmt_type'].replace('OBPROXY_T', 'T')
            if (self.is_dml_stmt(base_result['stmt_type'])):
                return (base_result['database_name'] == target_result['database_name'] \
                        and base_result['table_name'] == target_result['table_name'] \
                        and base_result['hint_query_timeout'] == target_result['hint_query_timeout']) \
                        or target_result['stmt_type'] == 'T_MULTI_STMT'
            elif (self.is_common_stmt(base_result['stmt_type'])):
                return True
            else:
                return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--expect", help="base result", action='store', dest='expect', default="mysqltest.result")
    parser.add_argument("-r", "--result", help="target result", action='store', dest='result', default="mysqltest.tmp")
    parser.add_argument("-m", "--mode", help="get sql mode", action='store', dest='mode', default="normal")
    args = parser.parse_args()

    MainHandler(args).run()
