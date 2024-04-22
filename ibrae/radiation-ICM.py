#coding: utf-8

import requests
import cx_Oracle
import datetime
from   bs4 import BeautifulSoup


# URL to data sources
URLS = {
	'Krasnoyarsk': (295, 73, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040101&channelID=510'),
	'Kansk':       (296, 74, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=ncktk9q1nod82cq9u5fekicqv5&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040102&channelID=511'),
	'Kozulka':     (297, 75, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040109&channelID=518'),
	'Achinsk':     (298, 76, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040103&channelID=512'),
	'Uzur':        (299, 77, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040107&channelID=516'),
	'Minusinsk':   (300, 78, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040106&channelID=515'),
	'Norilsk':     (301, 79, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040109&channelID=517'),
	'Eniseisk':    (302, 80, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040104&channelID=513'),
	'Lesosibirsk': (303, 81, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID=c6n7126980t49dh0rj0648vt67&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040105&channelID=514'),
	
}



def run():
	"""Run procedure for module Radiation"""
	
	# Parameters for Oracle DB connection
	ip   = '127.0.0.1'
	port =  1521
	SID  = 'oracle11'
	dsn_tns = cx_Oracle.makedsn(ip, port, SID)

	try:
		db  = cx_Oracle.connect('m4c', 'm4c', dsn_tns)
		cur = db.cursor()
	except:
		print 'NOT connected...'
		sys.exit(0)



	# Get current date value
	cur_date = datetime.date.today().strftime('%d.%m.%Y')


	#query = """truncate table data_rad2"""
	#cur.execute(query)
	#db.commit()

	# Iterate over sensors
	for city, url in URLS.items():

		# Download data from current data source...
		print 'Grab data - %s' % (city)
		html = requests.get(url[2], auth=('krsn','krsn')).content
		soup = BeautifulSoup(html, "html.parser")

		# Get rows from "measures" table... 
		tab_city = soup.find_all("table", { "id" : "measures" }).pop()
		trs = tab_city.find_all('tr')

		# Massive of passed values 
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
					
					# Date value is not equal current date - SKIP this...
					some_date = row[0].split()[0]
					if some_date!=cur_date:
						continue

					# Delete 2 element of massive
					row.pop(1)
					# Delete unit measure, change '.' symbol to ',' symbol
					tmp = row[1].split()[0]
					row[1] = float(tmp)

					# Save in global list of rows
					row.insert(0,url[1])
					row.insert(0,url[0])
					rows.append(row) 
				except:
					pass


		
		
		# Recalculate from milliZivers to microrentgen...
		rows = map(lambda (idstation, idsensor, date, value): (idstation, idsensor, date, value*100), rows)
		
		# Save to database...
		for idstation, idsensor, date, value in rows:
			query = """insert into data_rad2(IDSTATIONS, IDSENSORS, VAL, RDATE) values(%d, %d, %f, TO_TIMESTAMP('%s','DD.MM.YYYY HH24:MI:SS'))""" % (idstation, idsensor, value, date)
			print query
			cur.execute(query)

		db.commit()


	# Close DB connection
	cur.close()
	db.close()

# Start run procedure
run()
