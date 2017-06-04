# -*- encoding: utf-8 -*-
import pymysql
import datetime


resource_table = ['s20170322','s20170323','s20170324','s20170325','s20170326','s20170327','s20170328','s20170329','s20170330','s20170331','s20170401','s20170402','s20170403',
                's20170405','s20170407','s20170408','s20170409','s20170410','s20170411','s20170414','s20170415','s20170417','s20170418','s20170419']
def getdata(table):
    conn = pymysql.Connect(host='localhost',port=3306,user='root',passwd='admin',db='sanchuang',use_unicode=True, charset="utf8")
    cursor = conn.cursor()
    
    data = []
    query = 'select * from '+str(table)+' where arrive_place=\"深圳\" and depart_place=\"上海\"'
    cursor.execute(query)
    fetch = cursor.fetchall()
    dict = change_crawler_time(str(table))
    for each in fetch:
        list = []
        depart_time = str(each[5].strftime('%Y-%m-%d'))
        print(str(each[7].strftime('%Y-%m-%d %H:%M:%S')))
        print(dict[0])
        print(dict[1])
        scrappertime = dict[1][dict[0].index(str(each[7].strftime('%Y-%m-%d %H:%M:%S')))]
        scrappertime_temp = datetime.datetime.strptime(scrappertime,'%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d-%H-%M-%S')
        id = str(each[6])+'-'+str(scrappertime_temp)+'-'+str(depart_time)
        list.append(id)
        list.append(each[6])
        list.append(each[0])
        list.append(depart_time)
        list.append(scrappertime)
        data.append(list)
    
    conn.commit()
    cursor.close()
    conn.close()
    return data
    
def time_change(hour):
    if hour>=0 and hour<3:
        hour = 0
    elif hour>=3 and hour<6:
        hour = 3
    elif hour>=6 and hour<9:
        hour = 6
    elif hour>=9 and hour<12:
        hour = 9
    elif hour>=12 and hour<15:
        hour = 12
    elif hour>=15 and hour<18:
        hour = 15
    elif hour>=18 and hour<21:
        hour = 18
    elif hour>=21 and hour<24:
        hour = 21
    return hour

def change_crawler_time(table):
    conn = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='sanchuang',use_unicode=True, charset="utf8")
    cursor = conn.cursor()
    
    query = 'select distinct(scrappertime) from '+str(table)
    dict = []
    resource_list = []
    time_list = []
    list = []
    cursor.execute(query)
    for each in cursor.fetchall():
        resource_list.append(str(each[0].strftime('%Y-%m-%d %H:%M:%S')))
        time_list.append(time_change(each[0].hour))
    
    list.append(str(each[0].strftime('%Y-%m-%d '+str(time_list[0])+':00:00')))
    for i in range(1,len(time_list)):
        if time_list[i]-time_list[i-1]==0:
            time_list[i] = time_list[i-1] + 3
        else:
            pass
        list.append(str(each[0].strftime('%Y-%m-%d '+str(time_list[i])+':00:00')))
    
    dict.append(resource_list)
    dict.append(list)
        
    conn.commit()
    cursor.close()
    conn.close()
    return dict
    
def newdb():
#     namelist = getname()
    conn = pymysql.Connect(host='localhost',port=3306,user='root',passwd='admin',db='datacleaning',use_unicode=True, charset="utf8")
    cursor = conn.cursor()
#     for each in namelist:
#         create_table = 'create table if not exits '+str(each)+'id CHAR(30) NOT NULL PRIMARY KEY,airline CHAR(10) NOT NULL,price INT(5) NOT NULL,depart_time DATETIME NOT NULL,scrappertime DATETIEM NOT NULL'
#         cursor.execute(create_table)
    create_table = 'create table if not exists total(id CHAR(40) NOT NULL PRIMARY KEY,airline CHAR(10) NOT NULL,price INT(5) NOT NULL,depart_time DATETIME NOT NULL,scrappertime DATETIME NOT NULL)'
    cursor.execute(create_table)
    for table in resource_table:
        data = getdata(str(table))
        for each in data:
            print(each)
            try:
                insert = 'insert into total(id,airline,price,depart_time,scrappertime) values(\"'+str(each[0])+'\",\"'+str(each[1])+'\",\"'+str(each[2])+'\",\"'+str(each[3])+'\",\"'+str(each[4])+'\" )'
                cursor.execute(insert)
            except pymysql.err.IntegrityError:
                try:
                    insert = 'insert into total(id,airline,price,depart_time,scrappertime) values(\"'+str(each[0])+'\",\"'+str(each[1])+'\",\"'+str(each[2])+'\",\"'+str(each[3])+'\",\"'+str(each[4])+'\" )'
                    cursor.execute(insert)
                except pymysql.err.IntegrityError:
                    insert = 'insert into total(id,airline,price,depart_time,scrappertime) values(\"'+str(each[0])+'\",\"'+str(each[1])+'\",\"'+str(each[2])+'\",\"'+str(each[3])+'\",\"'+str(each[4])+'\" )'
                    cursor.execute(insert)
    conn.commit()
    cursor.close()
    conn.close()
    
    
def main():
    newdb()
    
    
if __name__ == '__main__':
    main()
