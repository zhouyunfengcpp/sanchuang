import pymysql
import time

#create table  sort:price,departcity,departtime,aircompany,arrivecity,arrivetime,airline,scrappertime
def create_scrapper_table(scrapper_table_name):
    #get connection
    connect = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='sanchuang')

    #use cursor
    cursor = connect.cursor()

    #create table
    a = cursor.execute('create table if not exists '+str(scrapper_table_name)+' (price int(5) NOT NULL,arrive_place VARCHAR(8) NOT NULL,arrive_time DATETIME NOT NULL,company VARCHAR(8) NOT NULL,depart_place VARCHAR(8) NOT NULL,depart_time DATETIME NOT NULL,airline VARCHAR(8) NOT NULL,scrappertime DATETIME NOT NULL,batch INT(5) NOT NULL,id CHAR(30) NOT NULL PRIMARY KEY)')
    print('create table successfully')
    #commit
    connect.commit()

    cursor.close()
    connect.close()
    
#insert data
def insert_data(scrapper_table_name,price,arrive_place,arrive_time,corp,depart_place,depart_time,fltno,scrappertime,batch,id):
    #get connection
    connect = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='sanchuang',use_unicode=True, charset="utf8")

    #use cursor
    cursor = connect.cursor()
    
    #console the problem if in one progression the table id created last day but insert today
    try:
        check = 'create table if not exists '+str(scrapper_table_name)+' (price int(5) NOT NULL,arrive_place VARCHAR(8) NOT NULL,arrive_time DATETIME NOT NULL,company VARCHAR(8) NOT NULL,depart_place VARCHAR(8) NOT NULL,depart_time DATETIME NOT NULL,airline VARCHAR(8) NOT NULL,scrappertime DATETIME NOT NULL,batch INT(5) NOT NULL,id CHAR(30) NOT NULL PRIMARY KEY)'  
        #insert data
        b = cursor.execute('insert into '+str(scrapper_table_name)+'(price,arrive_place,arrive_time,company,depart_place,depart_time,airline,scrappertime,batch,id) values('+price+',\''+arrive_place+'\',\''+arrive_time+'\',\''+corp+'\',\''+depart_place+'\',\''+depart_time+'\',\''+fltno+'\',\''+scrappertime+'\',\''+batch+'\',\''+id+'\')')
        print('insert data successfully')
        #commit
    except pymysql.err.IntegrityError:
        print('Duplicate entry \'airline-batch-count\' for key \'PRIMARY\'')
    connect.commit()

    cursor.close()
    connect.close()

