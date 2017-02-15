#encoding: utf-8
__author__ ='zhangyuanxiang'
from multiprocessing import Pool
import time
import sys
import MySQLdb
import MySQLdb.cursors
from optparse import OptionParser
import cx_Oracle
import hashlib
from sys import argv
reload(sys )
sys.setdefaultencoding('utf8')
error=[]
presult=[]

def get_cli_options():
    parser = OptionParser(usage="usage: python %prog [options]",description="""MySQL Table CheckSUM""")

    parser.add_option("-H", "--f_dsn",
                      dest="f",
                      default="127.0.0.1:3306:db:table",
                      metavar="host:port:db:table"
                      )

    parser.add_option("-L", "--t_dsn",
                      dest="t",
                      default="127.0.0.1:3306:db:table",
                      metavar="host:port:db:table"
                     )
    (options, args) = parser.parse_args()

    return options

def source_client(ip,port):
      try:
         con = MySQLdb.connect(host =ip,
                             user ='username',
                             passwd ='passwd',
                             port =int(port),
                             charset='utf8',
                             cursorclass = MySQLdb.cursors.SSCursor
                              )
      except Exception , e:
          print "source_client ",e
      return con
 
def source_table(db,table,ip,port):
       cur = source_client(ip,port).cursor()
       sql_1="select * from %s.%s where 0=1" %(db,table)
       row=""
       try:
          cur.execute(sql_1)
       except Exception , e:
          print "source_table ",e
       for i in range(0, len(cur.description)):
           if i==0:
                  row =row+cur.description[i][0]
           else:
                  row +=','+cur.description[i][0]
                  i=i+1
           cols="select "+"concat_ws"+"("+'"||"'+","+row+")"+" "+"from "+"%s.%s  " %(db,table)
       return cols


def destin_client(ip,port):
      try:
         con = MySQLdb.connect(host =ip,
                             user ='username',
                             passwd ='passwd',
                             port =int(port),
                             charset='utf8',
                             cursorclass = MySQLdb.cursors.SSCursor
                              )
      except Exception , e:
          print "destin_client ",e
      
      return con

def destin_table(db,table,ip,port):
       cur = destin_client(ip,port).cursor()
       sql_1="select * from %s.%s where 0=1" %(db,table)
       row=""
       try:
          cur.execute(sql_1)
       except Exception , e:
          print "destin_table ",e
       for i in range(0, len(cur.description)):
           if i==0:
                  row =row+cur.description[i][0]
           else:
                  row +=','+cur.description[i][0]
                  i=i+1
           cols="select "+"concat_ws"+"("+'"||"'+","+row+")"+" "+"from "+"%s.%s " %(db,table)
       return cols,"replace into %s.%s values " % (db,table)

def compare_row(source_1,destin_1):
    
   for s in source_1:
       if s not in destin_1 and s not in error:
          error.append(s)
       else:
          continue
   for l in destin_1:
       if l not in source_1:
          presult.append(l)
   for x in error:
       if x in destin_1 or x in presult:
          error.remove(x)   
       else:
          pass
     
def compare_table():
    batch=500
    lag=0
    options = get_cli_options()
    h=options.f
    j=options.t 
    f_h=h.strip().split(':')[0]
    f_p=h.strip().split(':')[1]
    f_d=h.strip().split(':')[2]  
    f_t=h.strip().split(':')[3]
    t_h=j.strip().split(':')[0]
    t_p=j.strip().split(':')[1]
    t_d=j.strip().split(':')[2]                 
    t_t=j.strip().split(':')[3]
    source_cur=source_client(f_h,f_p).cursor()
    destin_cur=destin_client(t_h,t_p).cursor()
    source_sql=source_table(f_d,f_t,f_h,f_p)
    destin_sql,replstr=destin_table(t_d,t_t,t_h,t_p)
    source_cur.execute(source_sql)
    destin_cur.execute(destin_sql)
    source_result = source_cur.fetchmany(batch) 
    while source_result: 
          destin_result = destin_cur.fetchmany(batch)
          source_md5=hashlib.md5(str(source_result)).hexdigest()
          destin_md5=hashlib.md5(str(destin_result)).hexdigest()
          if source_md5 !=destin_md5:
             compare_row(source_result,destin_result) 
          else:
             pass
          source_result=[]
          destin_result=[]
          source_result = source_cur.fetchmany(batch)
    for i in error:
        head='("'
        end='")'
        var_col=[]
        for var in list(i)[0].split("||"):
           var_col.append(var)
        strvar='","'.join(var_col)
        c_v=head+strvar+end
        insertsql= replstr+" "+c_v
        print insertsql
    
            
if __name__ == '__main__':
    compare_table()