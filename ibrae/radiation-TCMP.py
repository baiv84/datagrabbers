#coding: utf-8

import requests
import cx_Oracle
import datetime
from   bs4 import BeautifulSoup


# Session ID
PHP_SESS = 'ktebd9nc2g29lglpng6pgrlip0'


# URL to data sources
URLS = {
	'Krasnoyarsk':    (78, 301, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID={}&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040101&channelID=510'.format(PHP_SESS)),
	'Kansk':          (79, 302, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID={}&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040102&channelID=511'.format(PHP_SESS)),
	'Kozulka':        (80, 303, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID={}&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040109&channelID=518'.format(PHP_SESS)),
	'Achinsk':        (81, 304, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID={}&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040103&channelID=512'.format(PHP_SESS)),
	'Uzur':           (82, 305, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID={}&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040107&channelID=516'.format(PHP_SESS)),
	'Minusinsk':      (83, 306, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID={}&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040106&channelID=515'.format(PHP_SESS)),
	'Norilsk':        (84, 307, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID={}&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040109&channelID=517'.format(PHP_SESS)),
	'Eniseisk':       (85, 308, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID={}&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040104&channelID=513'.format(PHP_SESS)),
	'Lesosibirsk':    (86, 309, 'http://webmon.ibrae.ac.ru/live/Measures.php?PHPSESSID={}&MSID=%D0%9A%D1%80%D1%81%D0%BA&ID=24040105&channelID=514'.format(PHP_SESS)),
	
}


def run():
	"""Run procedure for module Radiation"""

	# Parameters for Oracle DB connection
	ip   = '10.116.10.30'
	port =  1521
	SID  = 'oracle11'
	dsn_tns = cx_Oracle.makedsn(ip, port, SID)

	try:
		db  = cx_Oracle.connect('m4c', 'm4c', dsn_tns)
		cur = db.cursor()
	except:
		print 'NOT connected...'
		sys.exit(0)

	# Iterate over sensors
	for city, url in URLS.items():
		# Skip over some sensors 51,52 stations NOT to grab from Inet...
		if url[2] == '':
			continue

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


		# Recalculate to microrentgen...
		rows = map(lambda (idstation, idsensor, date, value): (idstation, idsensor, date, float(value*100)), rows)
		
		# Save to database...
		for idstation, idsensor, date, value in rows:
			# Convert to Krasnoyarsk time format (MoscowTime + 4 hours)
			date  = datetime.datetime.strptime(date, "%d.%m.%Y %H:%M:%S")
			delta = datetime.timedelta(seconds=14400) # delta step = 4 hours
			date  = (date + delta).strftime("%d.%m.%Y %H:%M:%S")
			
			# Save data row to DB
			query = """insert into data_rad_ibrae(IDSTATIONS, IDSENSORS, VAL, RDATE) values(%d, %d, %f, TO_TIMESTAMP('%s','DD.MM.YYYY HH24:MI:SS'))""" % (idstation, idsensor, value, date)
			cur.execute(query)
		# Save row in DB
		db.commit()


	# Calculate middle values
	today = datetime.date.today()
	yestarday = (today-datetime.timedelta(days=1)).strftime('%d.%m.%Y')
	today = today.strftime('%d.%m.%Y')

	# Iterate over  stations
	for city, url in URLS.items():
		station = url[0]
		sensor  = url[1] 
		query = """select * from data_rad_ibrae where IDSTATIONS='%s' and IDSENSORS='%s' and RDATE >TO_TIMESTAMP('%s','DD.MM.YYYY') and RDATE<TO_TIMESTAMP('%s','DD.MM.YYYY')""" % (station,sensor,yestarday,today)
		cur.execute(query)
		rows = cur.fetchall()
		
		# Calculate maximum and middle value
		max_val = 0
		summa   = 0
		for (f1,f2,f3,f4) in rows:
			summa+=f3
			if f3 > max_val:
				max_val = f3
		# Do calculations
		try:
			average = float(summa)/len(rows)
			print 'City - %s, Maximum value - %s, Average value - %s' % (city, max_val, average)
			# Save data row to DB
			query = """insert into data_rad_ibrae_reduced(IDSTATIONS, IDSENSORS, AVERAGE, MAXVAL, RDATE) values(%d, %d, %f, %f,TO_TIMESTAMP('%s','DD.MM.YYYY'))""" % (url[0], url[1], average, max_val, yestarday)
			cur.execute(query)
		except:
			print 'We have no info about this post - %s' % (city)

	# Save rows to DB
	db.commit()

	# Close DB connection
	cur.close()
	db.close()

# Start run procedure
run()
