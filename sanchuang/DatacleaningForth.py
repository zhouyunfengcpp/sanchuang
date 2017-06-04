# -*- encodng: utf-8 -*-

import pymysql
import numpy as np
import pandas as pd

def getdata(table_name):
    conn = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='datacleaning',use_unicode=True, charset="utf8")
    cursor = conn.cursor()
    
    data = []
    cursor.execute('select * from '+str(table_name))
    for each in cursor.fetchall():
        list = []
        list.append(each[2])
        list.append(each[3])
        list.append(each[4].strftime('%Y-%m-%d %H:%M:%S'))
        list.append(each[5])
        data.append(list)
        
    conn.commit()
    cursor.close()
    conn.close()
    return data

def get_table_name():
    conn = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='datacleaning',use_unicode=True, charset="utf8")
    cursor = conn.cursor()
    
    list = []
    cursor.execute('show tables')
    for each in cursor.fetchall():
        list.append(each[0])
        
    list.remove('total')
    conn.commit()
    cursor.close()
    conn.close()
    return list

def doc():
    conn = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='datacleaning',use_unicode=True, charset="utf8")
    cursor = conn.cursor()
    
    for i in range(0,len(get_table_name())):
        data = getdata(get_table_name()[i])#depart_time
#         for i in range(0,len(data)):
#             if df.delta[i]<=0:
#                 df.delta[i] = 0
#             else:
#                 pass
            
#         for i in range(1,len(data)):
#             if df.delta[i]-df.delta[i-1]>3:
#                 insertRow = df[i-1]
#                 above = df[:i-1]
#                 blow = df[i:]
#                 df = pd.concat([above,insertRow,blow],ignore_index=True)

        cursor.execute('select distinct(airline) from '+str(get_table_name()[i]))
        airline = []#airline
        for a in cursor.fetchall():
            airline.append(a[0])
        for m in range(0,len(airline)):
            cursor.execute('select airline,price,depart_time,delta from '+str(get_table_name()[i])+' where airline=\"'+str(airline[m])+'\"')
            data = []
            for eachline in cursor.fetchall():
                list = []
                list.append(eachline[0])
                list.append(eachline[1])
                list.append(eachline[2])
                list.append(eachline[3])
                data.append(list)
            df = pd.DataFrame(data,columns=['airline','price','depart_time','delta'])
            f = open('C:\\Users\\15418\\Desktop\\sanchuang\\sanchuang\\data\\'+str(i+1)+'\\'+str(m+1)+'.xlsx','w')
            df.to_excel('C:\\Users\\15418\\Desktop\\sanchuang\\sanchuang\\data\\'+str(i+1)+'\\'+str(m+1)+'.xlsx',index=False)
        print(df)
        
    conn.commit()
    cursor.close()
    conn.close()
    return list

doc()