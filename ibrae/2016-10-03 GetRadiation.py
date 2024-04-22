#coding: utf-8

import requests
import xlwt
from   bs4 import BeautifulSoup


# URL to data sources
URLS = {
	'Krasnoyarsk': 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040101&channelID=510',
	'Kansk':       'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=ncktk9q1nod82cq9u5fekicqv5&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040102&channelID=511',
	'Achinsk':     'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040103&channelID=512',
	'Eniseisk':    'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040104&channelID=513',
	'Lesosibirsk': 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040105&channelID=514',
	'Minusinsk':   'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040106&channelID=515',
	'Uzur':        'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040107&channelID=516',
	'Norilsk':     'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040109&channelID=517',
	'Kozulka':     'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040109&channelID=518',	

}



def save_to_excel(items, fname):
	"""Save parsed lines to  Excel document.
	items - massive values to save.
	fname - name of file to save data,
	"""

	wb = xlwt.Workbook()
	ws = wb.add_sheet('Лист'.decode('utf8'))

	row = 0
	for item in items:
		cell = 0
		for elem in item:
			ws.write(row, cell, elem.decode('utf8'))
			cell+=1
		
		row +=1

	# Do save procedure
	wb.save(fname)




if __name__=='__main__':

	proxies = {
		"http":  "http://valera:valera@172.16.0.2:8080",
		"https": "http://valera:valera@172.16.0.2:8080",
	}

	for city, url in URLS.items():

		# Download data from current data source...
		print 'Grab data - %s' % (city)
		html = requests.get(url, auth=('krsn','krsn'), proxies=proxies).content
		soup = BeautifulSoup(html, "html.parser")

		# Get rows from "measures" table... 
		tab_city = soup.find_all("table", { "id" : "measures" }).pop()
		trs = tab_city.find_all('tr')

		# Massive of paesed values 
		rows = []
		# Iterate over rows
		for tr in trs:
			# skip 1 cell rows of table
			if len(tr.find_all('td'))==1: 
				continue
			else:
				try:
					row=[]
					# Get cells from 0-3 positions
					for i in range(0,3):
						cell = tr.find_all('td')[i].text.lstrip().rstrip().encode('utf8')
						row.append(cell)
					
					# Delete 2 element of massive
					row.pop(1)
					# Delete unit measure, change '.' symbol to ',' symbol
					tmp = row[1].split()[0]
					row[1] = tmp.replace('.', ',')

					# Save in global list of rows
					rows.append(row) 
				except:
					pass

		# Make data save procedure...
		print 'Save to file - %s.xls' % (city)
		save_to_excel(rows, 'e:\\'+city + '.xls')
