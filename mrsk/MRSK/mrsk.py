# -*- coding: utf-8 -*-

from   lxml import etree
import requests


# Request parameters to MRSK server
data = {
	"com":"otkl",
	"task":"select_planoff",
	"api":1
}




if __name__=='__main__':
        proxies = {
                "http":  "http://valera:valera@172.16.0.2:8080",
                "https": "http://valera:valera@172.16.0.2:8080",
        }

        # Send POST request to MRSK site
        print 'Send POST request to MRSK server...'
        r = requests.post('http://www.mrsk-sib.ru/geng/', data=data, proxies=proxies)
        print 'Server answer - %s.' % r.status_code
        items = r.json()

        # Get records for Krasnoyarsk region (reg_id=24) 
        elems24 = [item for item in items if item['reg_id']=='24' and item['gorod']!='']

        # Generate XML document...
        print '\nGenerate XML document...'
        root = etree.Element('root')
        for elem in elems24:
                obj24 = etree.SubElement(root, "object")
                obj_params = etree.SubElement(obj24, "obj_params")
		
                etree.SubElement(obj_params, "gorod").text = elem['gorod']
                etree.SubElement(obj_params, "raion").text = elem['raion']
                etree.SubElement(obj_params, "date_start").text = elem['date_start']
                etree.SubElement(obj_params, "date_finish").text = elem['date_finish'] 
                etree.SubElement(obj_params, "street").text = elem['street']
                etree.SubElement(obj_params, "home").text = elem['home']
                etree.SubElement(obj_params, "type_disable").text = elem['type_disable']

        # Save to XML file...
        print 'Save XML document to file - %s\n' % ('mrsksib.xml')
        out = etree.tostring(root, pretty_print=True, encoding='utf-8', xml_declaration=True)
        print out
        xml_out = open('2017120.xml', 'w')
        xml_out.write(out)
        xml_out.close()
