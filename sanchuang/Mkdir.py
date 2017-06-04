import os
import pymysql


def mkdirs():
    conn = pymysql.connect(host='localhost',port=3306,user='root',passwd='admin',db='datacleaning',use_unicode=True, charset="utf8")
    cursor = conn.cursor()

    a = cursor.execute('select distinct(depart_time) from total')
    
    for i in range(1,a+1):
        os.makedirs(r'C:\\Users\\15418\\Desktop\\sanchuang\\sanchuang\\data\\'+str(i))
        
    conn.commit()
    cursor.close()
    conn.close()


mkdirs()                 
