# -*- coding: utf-8 -*-
#主要处理通过数据库操作对于爬虫的数据按照格式化进行清理
import pymysql


#sort by airline
def sort_by_airline(name,batch):
    connect = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='sanchuang',use_unicode=True, charset="utf8")
    cursor = connect.cursor()
#     首先查询到每一个scrapper表当中特定航班的数据
    query = 'select airline from '+str(name)+' where batch = \''+str(batch)+'\' group by airline'
    cursor.execute(query)
    airline_list = []
    for each in cursor.fetchall():
        for e in each:
            airline_list.append(e)
    
# 已经查询到表当中对应batch的航班号的list，对应为airline_list
    for each in airline_list:
        query = 'select * from '+str(name)+' where airline = \''+str(each)+'\' and batch = \''+str(batch)+'\''
        cursor.execute(query)
        data = cursor.fetchall()
    
#         最后把查询到的信息填入新的表当中
        cursor.execute('create table if not exists '+str(each)+'(price INT(5) NOT NULL,arrive_time DATETIME NOT NULL,depart_time DATETIME NOT NULL,scrappertime DATETIME NOT NULL PRIMARY KEY)')
        for e in data:
            cursor.execute('insert into '+str(each)+'(price,arrive_time,depart_time,scrappertime) values ('+str(e[0])+',\''+str(e[2])+'\',\''+str(e[5])+'\',\''+str(e[7])+'\')')
    
    connect.commit()

    cursor.close()
    connect.close()

