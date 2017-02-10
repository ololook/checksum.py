#encoding: utf-8
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
def source_client():
      try:
         con = MySQLdb.connect(host ='127.0.0.1',
                             user ='usrname',
                             passwd ='passwd',
                             port =port,
                             charset='utf8',
                             cursorclass = MySQLdb.cursors.SSCursor
                              )
      except Exception , e:
          print "source_client ",e
      return con
 
def source_table():
       cur = source_client().cursor()
       sql_1="select * from gmonitor.backupset where 0=1"
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
           cols="select "+"concat_ws"+"("+'"||"'+","+row+")"+" "+"from "+"gmonitor.backupset"
       return cols


def destin_client():
      try:
         con = MySQLdb.connect(host ='127.0.0.1',
                             user ='usrname',
                             passwd ='passwd',
                             port =port,
                             charset='utf8',
                             cursorclass = MySQLdb.cursors.SSCursor
                              )
      except Exception , e:
          print "destin_client ",e
      
      return con

def destin_table():
       cur = destin_client().cursor()
       sql_1="select * from gmonitor.backupset_bak where 0=1"
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
           cols="select "+"concat_ws"+"("+'"||"'+","+row+")"+" "+"from "+"gmonitor.backupset_bak"
       return cols


def compare_row(source_1,destin_1,num):
    
    offset=num
    lg=len(source_1)
    for i in range(0,lg,offset):
        source1_md5=hashlib.md5(str(source_1[i:i+offset])).hexdigest()
        destin1_md5=hashlib.md5(str(destin_1[i:i+offset])).hexdigest()
        if offset==1 and source1_md5!=destin1_md5:
           print source_1[i:i+offset]
           print destin_1[i:i+offset]
           print 
         
        elif source1_md5!=destin1_md5:
            compare_row(source_1,destin_1,1)        
        else:
           pass

def compare_table():
    batch=25000
    lag=0
    source_cur=source_client().cursor()
    destin_cur=destin_client().cursor()
    source_sql=source_table()
    destin_sql=destin_table()
    source_cur.execute(source_sql)
    destin_cur.execute(destin_sql)
    source_result = source_cur.fetchmany(batch) 
    while source_result:
          lag=lag+1
          destin_result = destin_cur.fetchmany(batch)
          source_md5=hashlib.md5(str(source_result)).hexdigest()
          destin_md5=hashlib.md5(str(destin_result)).hexdigest()
          if source_md5 !=destin_md5:
             compare_row(source_result,destin_result,500)
          else:
             pass
          source_result=[]
          destin_result=[]
          source_result = source_cur.fetchmany(batch)

if __name__ == '__main__':
    compare_table()