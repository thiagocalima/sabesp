#! /usr/bin/python
import sys
import requests
import re
import time
from lxml import html
from datetime import timedelta, date, datetime
import redis


redisPool = redis.ConnectionPool(host='172.17.0.2', port=6379, db=0)


def redisGet(variableName):
    redisServer = redis.Redis(connection_pool=redisPool)
    response = redisServer.get(variableName)

    return response


def redisSet(variableName, variableValue):
    redisServer = redis.Redis(connection_pool=redisPool)
    redisServer.set(variableName, variableValue)


def redisDel(variableName, variableValue):
    redisServer = redis.Redis(connection_pool=redisPool)
    redisServer.delete(variableName, variableValue)


def redisHSet(variableName, variableKey, variableValue):
    redisServer = redis.Redis(connection_pool=redisPool)
    redisServer.hset(variableName, variableKey, variableValue)


def redisHGet(variableName, variableKey):
    redisServer = redis.Redis(connection_pool=redisPool)
    response = redisServer.hget(variableName, variableKey)

    return response


def redisHGet(variableName):
    redisServer = redis.Redis(connection_pool=redisPool)
    response = redisServer.hgetall(variableName)

    return response


def toString(obj):
    return ''.join(obj)


def parseValue(value):
    #revalue = re.compile('[0-9]+\,[0-9]*')
    revalue = re.compile('[-]*[0-9]+\,[0-9]*')
    value = revalue.findall(value)
    
    return value


def convertValueToNumber(value):
    value = toString(value)
    value = value.replace(",", ".")

    return value * 1


def daterange(startDate, endDate):
    for n in range(int ((endDate - startDate).days)):
        yield startDate + timedelta(n)


def getData(cmbDia, cmbMes, cmbAno):
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
    formData = {
                    '__VIEWSTATE':'A5VhumZhJMOVECoKmhuRbL+Do2a+QBpoIpipoW/vhIQv/4H+rOrMuDhreixzRFZst3PJzOauIVcOnkCecSYp+yhDdc+zuAbzottn+I7FnYc+MG6m8RpB6XtDmeRhUfe71Vn8udWKqvsE5l43o52rZS8BaQa0wx9tqFTAt6sENaBrWgBkzmOfSMVtF5n1UXIC3dMzugeY2QFObMghVlVYD74yEYLe+FfX8NgJks1EszFN3i1mfyA2B5Ur+wwsCpPv+6xvQtMS0TkDiReWdtyGHPcONupS26Mv7hqrnIOje9LYb5IMBVZfGjJDmxcoHjKRzLM8gyDzbzhKhMUlwS4dbtDZFC/BCnHxjrfK6FkM15TemqsWdU+j+7YYHX0V6A2nW7tAlVltJ2VImSk7aAhkRkJQZqoHKIsdIzG6vaJrPYDAA+5HCs/fhflcukH+Vhp9TpLbfPNXbOsNvPLTFI7LMUauyFT1MSZYkAlD1/bxRx48tblzlCpcIZEeQPsCk4Df02pMFIU3e3LmkCyjj6AnK8cTh9U/C9TXdQpPrqUcmH9yZLWE2WRbT0ZUsPf8q9EX6ajQLKfw9VqiMwpFt2oPMwQAlDaR+sRum0rTrCxg4wNQWK8ey3erhOZ+gES+8MA/jJ69NC/H+wTRA6b+2oiPbEnl5kAiUwVuoTDMv2pcWXnc/P2EorFFf7S+rDyS81S7i7xHUDDXiwYFkdNML6qdUUJc5ayzC3S4iC3DukPzOxTdGc4CiD+DLI5q3QapE6evh3ebF5rRnXGAWrQeb3/TJtzW8VIqwrIXi84FfcKWI0Q2XdIkJsxsxQi+Z5qJxS4M6awNSjpOQ7CHYht0GXh9vdNC4+zHN57DuiI1bLbmw2xaH7ZjNrohJBvAVd6l/rnjoJTvZHPrQOG7bvKGlj6Kwc+GbQqnONIdXeiYgC1y/cNQo78TE007wcrcsg01Kcv6N4tlaNP6/JanrMUjE7s+CsHCW1AIp54IpvWyj76D94Pq0OusU3eaX3rQNB0HbduuzsxpkkKo+fEgY6dL1u7ZtjfTV5Fyj2uMA/PET9CxP9DhCzOiRgCV05j+1L5b+c/3in92EGKksVKqFF2tNhFnUBTHVauqCMddBqBFFYRddHf7SBdRagbHQxkBNlzdot4ygunpo74FGBM1JhXBEmgEaEH70ZA0ZjCPofMU7Ju2HyuY6R8N7dLyOWs7wNC2wMU2d8Th8w==',
                    '__EVENTVALIDATION':'Ria5iwOm75okX8MEEkiLkl6i//GcHlEPiEMbw+Qp6Mjs89lArQr+UQn+apTmd/RmYeT3u+6KMZWiRbXnwhOTTSBleP0v2nmHHlj6cfNbuXGRUAjDe+3BkTZ7n/lWF9ukWDQ7X9wa0NfMsRhfERH+jR71imIBXEvOGt7LOpkvNPk+ectN+4tDY2N/bD9I9/XY6q4Ef2bkcKRtxzGGIzN0kNgmH3/bNvjEZZDBuQLw+QIKS6vrk1GkK+eOJSi17uT/bL2fQixUX8s2/m2FxYvucqRCbGv+GWaE329Z+AWZdnNjuPotbfGeFllblRmoia+3h0Ld8M4RtxF4/0G7iGMnxtadad/K1GSNNBKqAu9v43Q1XvUnre9MGxtRh1f7VZK1q/ErjK5CFGKGCcJkwbJ1z1nboC56yin/1J6/o3fRKHEvjRXROb+QOMW1YxPoqaoDhMaDJSbwYxJxFNHZwe5liNiRTAL7Rko7EmGeMYRI2SaFjV6pFAXc1nDNwlJVAVQS9tbEBtaicztyivFDcH9lpclY5zrmiSBYJ0nTxV24Hm7PNBvQ',
                    '__VIEWSTATEENCRYPTED':'',
                    'cmbDia': cmbDia,
                    'cmbMes': cmbMes,
                    'cmbAno': cmbAno,
                    'Imagebutton1.x': '7',
                    'Imagebutton1.y': '12'
                }
    indices = {
                    'nivel1':None, #Volume armazenado Nivel 1
                    'nivel2':None, #Volume armazenado Nivel 2
                    'nivel3':None, #Volume armazenado Nivel 3
                    'pluviometriaDia':None, #Pluviometria do dia
                    'pluviometriaMes':None, #Pluviometria acumulada no mes
                    'media':None #Media historica no mes
                }

    response = requests.post(url, data = formData, headers = header)
    htmlContent = toString(response.content)

    tree = html.fromstring(htmlContent.decode('utf-8'))

    countNivel = 0
    count = 0
    sistemaIndex = 0

    # Coletar Nivel 3
    for line in tree.xpath("//tr/td/font/text()"):
        valor = convertValueToNumber(parseValue(line))
        indices['nivel3'] = str(valor)

    # Reunindo os demais dados
    for line in tree.xpath("//tr/td/text()"):
        # Verificando se chave ou valor
        if ("%" not in line) and ("mm" not in line):
            None
        elif ("%" in line):
            valor = convertValueToNumber(parseValue(line))

            if (countNivel == 0):
                indices['nivel1'] = str(valor)
            elif (countNivel == 1):
                indices['nivel2'] = str(valor)

            countNivel = countNivel + 1

        elif ("mm" in line):
            countNivel = 0
            valor = convertValueToNumber(parseValue(line))

            if (count == 0):
                indices['pluviometriaDia'] = str(valor)
            elif (count == 1):
                indices['pluviometriaMes'] = str(valor)
            elif (count == 2):
                indices['media'] = str(valor)

            count = count + 1

            if (count > 2):
                count = 0

                redisChave = sistemas[sistemaIndex] + ":" + cmbDia + "/" + cmbMes + "/" + cmbAno

                for key, value in indices.items():
                    redisHSet(redisChave, key, value)
                
                sistemaIndex = sistemaIndex + 1

    
    redisSet("last", cmbDia + "/" + cmbMes + "/" + cmbAno)

    # Esperando alguns segundos para coletar proximo batch
    #time.sleep(10)


def getLast():
    return redisGet("last")


def main():    
    today = datetime.now()
    
    if (getLast() is None):
        startDate = date(2003, 6, 21)
    elif (getLast() is not None):
        startDate = datetime.strptime(getLast(), '%d/%m/%Y')
        startDate = date(startDate.year, startDate.month, startDate.day)
    
    endDate = date(today.year, today.month, today.day)

    for singleDate in daterange(startDate, endDate):
        day = str(singleDate.strftime("%d")).lstrip('0')
        month = str(singleDate.strftime("%m")).lstrip('0')
        year = str(singleDate.strftime("%Y"))
        
        getData(day, month, year)


if __name__ == '__main__':
    sys.exit(main())