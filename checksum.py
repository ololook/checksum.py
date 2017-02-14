from multiprocessing import Pool
import time
import sys
import MySQLdb
import MySQLdb.cursors
import cx_Oracle
import hashlib
from sys import argv
reload(sys )
sys.setdefaultencoding('utf8')
error=[]
presult=[]
def source_client():
      try:
         con = MySQLdb.connect(host ='127.0.0.1',
                             user ='username',
                             passwd ='passwd',
                             port =7307,
                             charset='utf8',
                             cursorclass = MySQLdb.cursors.SSCursor
                              )
      except Exception , e:
          print "source_client ",e
      return con
 
def source_table():
       cur = source_client().cursor()
       sql_1="select * from gmonitor.test_01 where 0=1"
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
           cols="select "+"concat_ws"+"("+'"||"'+","+row+")"+" "+"from "+"gmonitor.test_01 order by id "
       return cols


def destin_client():
      try:
         con = MySQLdb.connect(host ='127.0.0.1',
                             user ='username',
                             passwd ='passwd',
                             port =7307,
                             charset='utf8',
                             cursorclass = MySQLdb.cursors.SSCursor
                              )
      except Exception , e:
          print "destin_client ",e
      
      return con

def destin_table():
       cur = destin_client().cursor()
       sql_1="select * from gmonitor.test_02 where 0=1"
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
           cols="select "+"concat_ws"+"("+'"||"'+","+row+")"+" "+"from "+"gmonitor.test_02 order by id"
       return cols

def remove_values_from_list(the_list, val):
    return [value for value in the_list if value != val]

def compare_row(source_1,destin_1,num):
    offset=num
    lg=len(source_1)
    for i in range(0,lg,offset):
        destin2=destin_1[i:i+offset]
        source2=source_1[i:i+offset]
        source1_md5=hashlib.md5(str(source2)).hexdigest()
        destin1_md5=hashlib.md5(str(destin2)).hexdigest()
        if  source1_md5 != destin1_md5:
           for s in source2:
               s_md=hashlib.md5(str(s)).hexdigest()
               for d in destin2:
                   d_md=hashlib.md5(str(d)).hexdigest()
                   if s_md==d_md:
                      while s in error: 
                            error.remove(s)
                      break
                   else:
                      if s not in error:
                         error.append(s)
        else:   
           continue
    for x in error:
        x_md=hashlib.md5(str(x)).hexdigest()
        for y in destin_1:
            y_md=hashlib.md5(str(y)).hexdigest()
            if x_md==y_md:
               while x in error: 
                    error.remove(x)
               break
            else:
               pass
def compare_table():
    batch=500
    lag=0
    source_cur=source_client().cursor()
    destin_cur=destin_client().cursor()
    source_sql=source_table()
    destin_sql=destin_table()
    source_cur.execute(source_sql)
    destin_cur.execute(destin_sql)
    source_result = source_cur.fetchmany(batch) 
    while source_result: 
          destin_result = destin_cur.fetchmany(batch)
          source_md5=hashlib.md5(str(source_result)).hexdigest()
          destin_md5=hashlib.md5(str(destin_result)).hexdigest()
          if source_md5 !=destin_md5:
             compare_row(source_result,destin_result,100) 
          else:
             pass
          source_result=[]
          destin_result=[]
          source_result = source_cur.fetchmany(batch)
    for i in error:
        print i
if __name__ == '__main__':
    compare_table()
