#coding: utf-8

import requests
import xlwt
from   bs4 import BeautifulSoup


# URL шаблон для подгрузки данных
pereprava_url = 'http://ois.krudor.ru/oi/'




def write_to_excel(items, fname):
    wb = xlwt.Workbook()
    ws = wb.add_sheet('Лист'.decode('utf8'))

    row = 0
    for item in items:
        cell = 0
        for elem in item:
            ws.write(row, cell, elem.decode('utf8'))
            cell+=1
        row+=1

    # Сохраняем в Excel
    wb.save(fname)



if __name__=='__main__':
    
    proxies = {
        "http":  "http://valera:valera@172.16.0.2:8080",
        "https": "http://valera:valera@172.16.0.2:8080",
    }
	
    # Подгружаем всю страницу КРУДОР
    html = requests.get(pereprava_url, auth=('mchs_monitoring','kyDCc0'), proxies=proxies).content        
    soup = BeautifulSoup(html, "html.parser")
    

    #**************************ПЕРЕПРАВЫ************************************************
    # Получаем объекты из таблицы Переправа
    tab_pereprava = soup.find_all("table", { "id" : "bridges_in_plan_list" }).pop()
    trs = tab_pereprava.find_all('tr')
    
    # Массив распарсенных значений из таблицы Переправа
    rows = []
    # Итерируем по таблице Переправа
    for tr in trs:
        if len(tr.find_all('td'))==1: 
            continue
        else:
            try:
                row=[]
                for i in range(0,8):
                    cell = tr.find_all('td')[i].text.lstrip().rstrip().encode('utf8')
                    row.append(cell)
                rows.append(row) 
            except:
                pass
                

    print 'Save pereprava to file...'
    write_to_excel(rows,'pereprava.xls')



    #*********************ЗИМНИКИ*****************************************************
    # Получаем объекты из таблицы Зимники
    tab_zimnik = soup.find_all("table", { "id" : "winter_in_plan_list" }).pop()
    trs = tab_zimnik.find_all('tr')
    
    # Массив распарсенных значений из таблицы Переправа
    rows = []
    # Итерируем по таблице Переправа
    for tr in trs:
        if len(tr.find_all('td'))==1: 
            continue
        else:
            try:
                row=[]
                for i in range(0,4):
                    cell = tr.find_all('td')[i].text.lstrip().rstrip().encode('utf8')
                    row.append(cell)
                rows.append(row) 
            except:
                pass
                

    print 'Save zimniki to file...'
    write_to_excel(rows,'zimniki.xls')


    #************************ДТП**********************************************
    # Получаем объекты из таблицы ДТП
    tab_dtp = soup.find_all("table", { "id" : "dtp_list" }).pop()
    trs = tab_dtp.find_all('tr')
    
    # Массив распарсенных значений из таблицы ДТП
    rows = []
    # Итерируем по таблице ДТП
    for tr in trs:
        if len(tr.find_all('td'))==1: 
            continue
        else:
            try:
                row=[]
                for i in range(0,13):
                    cell = tr.find_all('td')[i].text.lstrip().rstrip().encode('utf8')
                    row.append(cell)
                rows.append(row) 
            except:
                pass
                

    print 'Save DTP to file...'
    write_to_excel(rows,'dtp.xls')


    #************************ЧС**********************************************
    # Получаем объекты из таблицы ЧС
    tab_chs = soup.find_all("table", { "id" : "chs_list" }).pop()
    trs = tab_chs.find_all('tr')
    
    # Массив распарсенных значений из таблицы ЧС
    rows = []
    # Итерируем по таблице ЧС
    for tr in trs:
        if len(tr.find_all('td'))==1: 
            continue
        else:
            try:
                row=[]
                for i in range(0,12):
                    cell = tr.find_all('td')[i].text.lstrip().rstrip().encode('utf8')
                    row.append(cell)
                rows.append(row) 
            except:
                pass
                

    print 'Save CHS to file...'
    write_to_excel(rows,'chs.xls')
