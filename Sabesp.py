#! /usr/bin/python
import sys
import requests
import re
import time
from lxml import html
from datetime import timedelta, date, datetime

def toString(obj):
    return ''.join(obj)

def parseValue(value):
    revalue = re.compile('[0-9]+\,[0-9]*')
    value = revalue.findall(value)
    
    return value

def convertValueToNumber(value):
    value = toString(value)
    value = value.replace(",", ".")

    return value * 1

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_data(cmbDia, cmbMes, cmbAno, csvfile):
    try:
        csvfile = open(csvfile, 'a')
    except IOError, e:
        print ("Failed to find file " + csvfile)
        sys.exit(1)

    url = 'http://www2.sabesp.com.br/mananciais/DivulgacaoSiteSabesp.aspx'
    header = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                'Accept-Encoding': 'none',
                'Accept-Language': 'en-US,en;q=0.8',
                'Connection': 'keep-alive'
            }
    sistemas = [
                    'Cantareira',
                    'Alto Tiete',
                    'Guarapiranga',
                    'Alto Cotia',
                    'Rio Grande',
                    'Rio Claro'
                ]
    form_data = {
                    '__VIEWSTATE':'/wEPDwUJNDM2NTc5MjA3D2QWAgIBD2QWCGYPDxYCHgRUZXh0BQowMy8wMS8yMDE2ZGQCAQ8QZA8WH2YCAQICAgMCBAIFAgYCBwIIAgkCCgILAgwCDQIOAg8CEAIRAhICEwIUAhUCFgIXAhgCGQIaAhsCHAIdAh4WHxAFATEFATFnEAUBMgUBMmcQBQEzBQEzZxAFATQFATRnEAUBNQUBNWcQBQE2BQE2ZxAFATcFATdnEAUBOAUBOGcQBQE5BQE5ZxAFAjEwBQIxMGcQBQIxMQUCMTFnEAUCMTIFAjEyZxAFAjEzBQIxM2cQBQIxNAUCMTRnEAUCMTUFAjE1ZxAFAjE2BQIxNmcQBQIxNwUCMTdnEAUCMTgFAjE4ZxAFAjE5BQIxOWcQBQIyMAUCMjBnEAUCMjEFAjIxZxAFAjIyBQIyMmcQBQIyMwUCMjNnEAUCMjQFAjI0ZxAFAjI1BQIyNWcQBQIyNgUCMjZnEAUCMjcFAjI3ZxAFAjI4BQIyOGcQBQIyOQUCMjlnEAUCMzAFAjMwZxAFAjMxBQIzMWdkZAICDxBkDxYMZgIBAgICAwIEAgUCBgIHAggCCQIKAgsWDBAFA2phbgUBMWcQBQNmZXYFATJnEAUDbWFyBQEzZxAFA2FicgUBNGcQBQNtYWkFATVnEAUDanVuBQE2ZxAFA2p1bAUBN2cQBQNhZ28FAThnEAUDc2V0BQE5ZxAFA291dAUCMTBnEAUDbm92BQIxMWcQBQNkZXoFAjEyZ2RkAgMPEGQPFg5mAgECAgIDAgQCBQIGAgcCCAIJAgoCCwIMAg0WDhAFBDIwMTYFBDIwMTZnEAUEMjAxNQUEMjAxNWcQBQQyMDE0BQQyMDE0ZxAFBDIwMTMFBDIwMTNnEAUEMjAxMgUEMjAxMmcQBQQyMDExBQQyMDExZxAFBDIwMTAFBDIwMTBnEAUEMjAwOQUEMjAwOWcQBQQyMDA4BQQyMDA4ZxAFBDIwMDcFBDIwMDdnEAUEMjAwNgUEMjAwNmcQBQQyMDA1BQQyMDA1ZxAFBDIwMDQFBDIwMDRnEAUEMjAwMwUEMjAwM2dkZBgBBR5fX0NvbnRyb2xzUmVxdWlyZVBvc3RCYWNrS2V5X18WAQUMSW1hZ2VidXR0b24x8WdutFK5Le++2jWcURTQSEnKz3I=',
                    '__EVENTVALIDATION':'/wEWOwLpkqXvBgKD1676AgKC1676AgKB1676AgKA1676AgKH1676AgKG1676AgKF1676AgKU1676AgKb1676AgKD1+75AgKD1+L5AgKD1+b5AgKD19r5AgKD1975AgKD19L5AgKD19b5AgKD18r5AgKD1476AgKD14L6AgKC1+75AgKC1+L5AgKC1+b5AgKC19r5AgKC1975AgKC19L5AgKC19b5AgKC18r5AgKC1476AgKC14L6AgKB1+75AgKB1+L5AgKX1/L8BwKW1/L8BwKV1/L8BwKU1/L8BwKT1/L8BwKS1/L8BwKR1/L8BwKA1/L8BwKP1/L8BwKX17L/BwKX177/BwKX17r/BwLOr4vcDQLOr//wBgLOr+OVDwLOr9euCALOr7tDAs6vr+QJAs6vk7kCAqWWoaAFAqWWlcUNAqWWuawLAqWWrcEDAqWWkZoMAqWWhb8FAqWW6dMNAtLCmbQIz0MOMHTs34kAum/fHTWwFy9+gUc=',
                    'cmbDia': cmbDia,
                    'cmbMes': cmbMes,
                    'cmbAno': cmbAno,
                    'Imagebutton1.x': '8',
                    'Imagebutton1.y': '13'
                }

    response = requests.post(url, data = form_data, headers = header)

    htmlContent = toString(response.content)
    tree = html.fromstring(htmlContent.decode('utf-8'))

    count = 0
    sistemaIndex = 0
    result = ''

    # Coletar Nivel 3
    for line in tree.xpath("//tr/td/font/text()"):
        valor = convertValueToNumber(parseValue(line))
        result = result + ";" + str(valor)

    # Reunindo os demais dados
    for line in tree.xpath("//tr/td/text()"):
        # Verificando se chave ou valor
        if ("%" not in line) and ("mm" not in line):
            None
        elif ("%" in line):
            valor = convertValueToNumber(parseValue(line))
            result = result + ";" + str(valor)
        elif ("mm" in line):
            valor = convertValueToNumber(parseValue(line))

            if (count == 0):
                result = str(valor) + result
            elif (count > 0):
                result = str(valor) + ";" + result

            count = count + 1

            if (count > 2):
                count = 0
                csvfile.write(cmbDia + "/" + cmbMes + "/" + cmbAno + ";" + sistemas[sistemaIndex] + ";" + result + "\n")
                sistemaIndex = sistemaIndex + 1
                result = ''

    csvfile.close()

    # Esperando alguns segundos para coletar proximo batch
    # time.sleep(10)

def main():
    
    today = datetime.now()
    start_date = date(2003, 1, 1)
    end_date = date (today.year, today.month, today.day)

    for single_date in daterange(start_date, end_date):
        day = str(single_date.strftime("%d")).lstrip('0')
        month = str(single_date.strftime("%m")).lstrip('0')
        year = str(single_date.strftime("%Y"))
        
        get_data(day, month, year, sys.argv[1])

if __name__ == '__main__':
    sys.exit(main())
