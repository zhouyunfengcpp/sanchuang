import json
import requests
import requests.packages
import time
import csv
import datetime
import threading
import os
import sanchuang.MySQLchange as sql
import sanchuang.Datacleaning as dc


#by http proyocol,get json data
def get_data(url,scrappertime):
    useragent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML+ like Gecko) Chrome/55.0.2883.87 Safari/537.36'
    host = 'flight.elong.com'
    cookie = 'CookieGuid=30197e3a-ed27-466c-b484-d8c285b3da86; SessionGuid=96efeace-ea3f-4ff8-b560-6a09601c5c5a; Esid=ddfc39d1-5634-4cb6-80d8-8ce9d3332535; ASP.NET_SessionId=srhatt45xpwa0a3x1w3cmg45; route=ddf9bf488d0bcf3442966ba1007d7092; FlightCondition=ArrName=%25E4%25B8%258A%25E6%25B5%25B7%7CShanghai&DepName=%25E5%258C%2597%25E4%25BA%25AC%7CBeijing&DepDate=2017-03-19&ArrCode=SHA&DepCode=BJS&FType=0&DateType=Domestic&; JSESSIONID=05654C63CE4A1C8BB0CBCCCDC5DE944A; com.eLong.CommonService.OrderFromCookieInfo="Status=1&Orderfromtype=5&Isusefparam=0&Pkid=50793&Parentid=3150&Coefficient=0.0&Makecomefrom=0&Cookiesdays=0&Savecookies=0&Priority=9000"; s_cc=true; Hm_lvt_a8cab4bd1a0039ff303b048b92b77538=1488261203,1489370805,1489570413,1489845077; Hm_lpvt_a8cab4bd1a0039ff303b048b92b77538=1489845275; s_visit=1; s_sq=%5B%5BB%5D%5D'
    header = {'Host':host,'User-Agent':useragent,'Cookie':cookie}
    try:
        req = requests.get(url,headers=header)
        info = json.loads(req.text)['value']['MainLegs']
        data = []
        for li in info:
            price = li['cabs'][0]['price']
            arrive_place = li['segs'][0]['acn']
            arrive_time = li['segs'][0]['atime']
            corp = li['segs'][0]['corpn']
            depart_place = li['segs'][0]['dcn']
            depart_time = li['segs'][0]['dtime']
            fltno = li['segs'][0]['fltno']  
                    
            list = []
        #         list.append(str(price))
        #         list.append(arrive_place.encode('gbk','ignore'))
        #         list.append(arrive_time.encode('gbk','ignore'))
        #         list.append(corp.encode('gbk','ignore'))
        #         list.append(depart_place.encode('gbk','ignore'))
        #         list.append(depart_time.encode('gbk','ignore'))
        #         list.append(fltno.encode('gbk','ignore'))
        #         list.append(str(scrappertime).encode('gbk','ignore'))
        #         list.append(id.encode('gbk','ignore'))
            list.append(str(price))
            list.append(arrive_place)
            list.append(arrive_time)
            list.append(corp)
            list.append(depart_place)
            list.append(depart_time)
            list.append(fltno)
            list.append(scrappertime)
            data.append(list)
        return data
    except KeyError:
        return False
    except requests.exceptions.ConnectionError:
        return True
    except requests.packages.urllib3.exceptions.ProtocolError:
        return True
    except ConnectionResetError:
        return True
        
#change the arrivecity departcity,time,daycount to get url
def get_url(ArriveCity,DepartCity,Year,Month,Day,DayCount):
    Name = {'BJS':'%E5%8C%97%E4%BA%AC','SHA':'%E4%B8%8A%E6%B5%B7','SZX':'%E6%B7%B1%E5%9C%B3','XMN':'%E5%8E%A6%E9%97%A8'}
    NameEn = {'BJS':'Beijing','SHA':'Shanghai','SZX':'Shenzhen','XMN':'Xiamen'}
    NowTime = time.strftime("%Y-%m-%d")
    url = 'http://flight.elong.com/jajax/OneWay/S?AirCorp=0&ArriveCity='+ArriveCity+'&ArriveCitiName='+Name[ArriveCity]+'&ArriveCityNameEn='+NameEn[ArriveCity]+'&BackDayCount=4&DayCount='+DayCount+'&DepartCity='+DepartCity+'&DepartCityName='+Name[DepartCity]+'&DepartCityNameEn='+NameEn[DepartCity]+'&DepartDate='+Year+'%2F'+Month+'%2F'+Day+'+00%3A00&FlightType=OneWay&OrderBy=Price&PageName=list&SeatLevel=Y&serviceTime='+str(NowTime)
    return url

#action the method above and get data to write in MySQL(just insert data ,not creat tables)
def action(batch,scrappertime,scrapper_table_name):
    citylist = ['BJS','SHA','SZX','XMN']
    daylist = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']
    monthlist = ['01','02','03','04','05','06','07','08','09','10','11','12']
    today = datetime.date.today()
    depart_city_num = 0
    arrive_city_num = 0
    day_num = today.day-1
    month_num = today.month-1
    url = ''
    for depart_city_num in range(0,4):
        for arrive_city_num in range(depart_city_num+1,4):
            count = 0
            while count<=40:
                url = get_url(citylist[arrive_city_num], citylist[depart_city_num], Year='2017', Month=monthlist[month_num], Day=daylist[day_num],DayCount=str(count))
                print(citylist[arrive_city_num])
                print(arrive_city_num)
                print(count)
                data = get_data(url,scrappertime)
                count = count +1
                day = datetime.datetime.strptime(scrappertime,'%Y-%m-%d %H:%M:%S') + datetime.timedelta(days=count)
                month_num = day.month-1
                day_num = day.day-1
                print('success get data')
                if data!=False and data!=True: 
                    for each in data:
                        id = each[6]+'-'+str(batch)+'-'+str(count)
                        sql.insert_data(scrapper_table_name,each[0], each[1], each[2], each[3], each[4], each[5], each[6], each[7], batch,str(id))
                elif data==True:
                    data=get_data(url)
                    if data!=False and data!=True: 
                        for each in data:
                            id = each[6]+'-'+str(day.strftime('%Y%m%d%H'))+'-'+str(batch)+'-'+str(count)
                            sql.insert_data(scrapper_table_name,each[0], each[1], each[2], each[3], each[4], each[5], each[6], each[7], batch,str(id))
                elif data==False:
                    pass
                    
                    
#create table to prepare for the threading
def run():
    batch = 1
    while batch!=0:
        scrappertime = time.strftime('%Y-%m-%d %H:%M:%S')
        scrapper_table_name = 's'+scrappertime[0:4]+scrappertime[5:7]+scrappertime[8:10] 
        sql.create_scrapper_table(scrapper_table_name)
        action(str(batch),scrappertime,scrapper_table_name)
        dc.sort_by_airline(scrapper_table_name, batch)
        batch = batch + 1
        time.sleep(10800)
        
#use threading        
def main():
    t = threading.Thread(target=run,args=(),name='sanchuang')
    t.start()
    

if __name__ == '__main__':
    main()