import pymysql
import time

#create table  sort:price,departcity,departtime,aircompany,arrivecity,arrivetime,airline,scrappertime
def create_table():
    #get connection
    connect = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='sanchuang')

    #use cursor
    cursor = connect.cursor()

    #create table
    name = time.strftime('%Y%m%d')
    a = cursor.execute('create table if not exists s'+str(name)+' (price int(5) NOT NULL,arrive_place VARCHAR(8) NOT NULL,arrive_time DATETIME NOT NULL,company VARCHAR(8) NOT NULL,depart_place VARCHAR(8) NOT NULL,depart_time DATETIME NOT NULL,airline VARCHAR(8) NOT NULL,scrappertime DATETIME NOT NULL,batch INT(5) NOT NULL,id CHAR(30) NOT NULL PRIMARY KEY)')
    print('create table successfully')
    #commit
    connect.commit()

    cursor.close()
    connect.close()
    
#insert data
def insert_data(price,arrive_place,arrive_time,corp,depart_place,depart_time,fltno,scrappertime,batch,id):
    #get connection
    connect = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='sanchuang',use_unicode=True, charset="utf8")

    #use cursor
    cursor = connect.cursor()

    #insert data
    name = time.strftime('%Y%m%d')
    b = cursor.execute('insert into s'+str(name)+'(price,arrive_place,arrive_time,company,depart_place,depart_time,airline,scrappertime,batch,id) values('+price+',\''+arrive_place+'\',\''+arrive_time+'\',\''+corp+'\',\''+depart_place+'\',\''+depart_time+'\',\''+fltno+'\',\''+scrappertime+'\',\''+batch+'\',\''+id+'\')')
    print('insert data successfully')
    #commit
    connect.commit()

    cursor.close()
    connect.close()

