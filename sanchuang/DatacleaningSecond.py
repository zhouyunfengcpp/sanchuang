# -*- encoding: utf-8 -*-

import pymysql
import datetime

def classify():
    conn = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='datacleaning',use_unicode=True, charset="utf8")
    cursor = conn.cursor()
    
    cursor.execute('select distinct(depart_time) from total')
    depart_time = []
    for each in cursor.fetchall():
        depart_time.append(each[0].strftime('%Y-%m-%d %H:%M:%S'))
    
    airline = []
    for each in depart_time:
        cursor.execute('select distinct(airline) from total where depart_time=\"'+str(each)+'\"')
        airlines = []
        for e in cursor.fetchall():
            airlines.append(e[0])
        airline.append(airlines)
     
     
    for i in range(1,len(airline)-1):
        for each in airline[i]:
            if each in airline[i-1]:
                pass
            else:
                delete = 'delete from total where airline=\"'+str(each)+'\"'
                cursor.execute(delete)
                 
                 
    for i in range(0,len(airline)-2):
        for each in airline[i]:
            if each in airline[i+1]:
                pass
            else:
                delete = 'delete from total where airline=\"'+str(each)+'\"'
                cursor.execute(delete)
                
    for each in airline:
        print(each)
    
    
    conn.commit()
    cursor.close()
    conn.close()
    
    
classify()