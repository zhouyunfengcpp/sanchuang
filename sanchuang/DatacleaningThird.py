# -*- encoding: utf-8 -*-
import pymysql
import os
import numpy as np
import pandas as pd


def getdata(depart_time):
    conn = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='datacleaning',use_unicode=True, charset="utf8")
    cursor = conn.cursor()
    
    data = []
    cursor.execute('select * from total where depart_time=\"'+str(depart_time)+'\"')
    for eachline in cursor.fetchall():
        list = []
        list.append(eachline[0])#id
        hour = int((eachline[3]-eachline[4]).days*24+(eachline[3]-eachline[4]).seconds/3600)
        list.append(eachline[4].strftime('%Y-%m-%d %H:%M:%S'))#scrappertime
        list.append(eachline[1])#airline
        list.append(eachline[2])#price
        list.append(eachline[3].strftime('%Y-%m-%d %H:%M:%S'))#depart_time
        list.append(hour)#delta
        data.append(list)
        
    conn.commit()
    cursor.close()
    conn.close()
    return data
    

def create_table():
    conn = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='datacleaning',use_unicode=True, charset="utf8")
    cursor = conn.cursor()
    
    cursor.execute('select distinct(depart_time) from total')
    for eachline in cursor.fetchall():
        depart_time = eachline[0]
        table_name = depart_time.strftime('%Y%m%d')
        data = getdata(depart_time.strftime('%Y-%m-%d %H:%M:%S'))
        create = 'create table if not exists d'+str(table_name)+'(id CHAR(40) NOT NULL PRIMARY KEY,scrappertime DATETIME NULL,airline CHAR(10) NULL,price INT(5) NULL,depart_time DATETIME NULL,delta INT(4) NULL)'
        cursor.execute(create)
        for each in data:
            insert = 'insert into d'+str(table_name)+'(id,scrappertime,airline,price,depart_time,delta) values (\"'+str(each[0])+'\",\"'+str(each[1])+'\",\"'+str(each[2])+'\",\"'+str(each[3])+'\",\"'+str(each[4])+'\",\"'+str(each[5])+'\")'
            cursor.execute(insert)
    conn.commit()
    cursor.close()
    conn.close()
    return data
    

def doc():
    conn = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='datacleaning',use_unicode=True, charset="utf8")
    cursor = conn.cursor()
    
    cursor.execute('show tables')
    table = []
    for each in cursor.fetchall():
        table.append(each[0])
    table.remove('total')
    
    for each in table:
        data = getdata(str(each))
        name = []
        cursor.execute('show columns from '+str(each))
        for a in cursor.fetchall():
            name.append(a[0])
        df = pd.DataFrame(data,columns=name)
        text_name = 'C:\\Users\\15418\\Desktop\\sanchuang\\sanchuang\\data\\'+str(each)+'.csv'
        f = open(text_name,'w')
        f.close
        df.to_csv(text_name,index=False,sep=',')
    
    conn.commit()
    cursor.close()
    conn.close()
    
create_table()