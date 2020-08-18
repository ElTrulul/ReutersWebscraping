import constants as cs
import d00_utils as ut

import requests
import csv
import time


def read_rics_from_csv(csvfile):
    #Import Reuters-Shorts from csv
    with open(csvfile) as shorts:
        RICs = list(csv.reader(shorts))[1:]
    print('RICs imported from CSV.')
    return RICs


def gethtml(url):
    r = requests.get(url)
    return r.content.decode('utf-8')


def scrapecompaniesfromreuters(shorts, dbparameters, datatype):
    #Definition of Variables
    totalno = len(shorts)
    count = 0
    errorlist = []
    todaydate = int(time.time())
    if datatype == 'fin':
        for ric in shorts:
            ric = ric[0]
            count += 1
            for addurl in cs.ADDURLS_TO_TABLENAMES:
                url = cs.ROOTURL+ric+addurl
                try:
                    html = gethtml(url)
                    ut.writedatabase(cs.ADDURLS_TO_TABLENAMES[addurl], (ric, todaydate, url, html), dbparameters)
                    print(f'Company {count}/{totalno} ({ric}) scraped and added to table {cs.ADDURLS_TO_TABLENAMES[addurl]}. URL: {url}')
                except:
                    errorlist.append(ric)
                    html = ""
                    print(f'Company {count}/{totalno} ({ric}) scraping failure. URL: {url}')       
    elif datatype == 'mar':
        #Stockdata
        for ric in shorts:
            ric = ric[0]
            count += 1
            url = cs.ROOTURL+ric
            try:
                html = gethtml(url)
                ut.writedatabase(cs.STOCKDATA, (ric, todaydate, url, html), dbparameters)
                print(f'Company {count}/{totalno} ({ric}) scraped and added to table {cs.STOCKDATA}. URL: {url}')
            except:
                errorlist.append(ric)
                html = ""
                print(f'Company {count}/{totalno} ({ric}) scraping failure. URL: {url}') 
        #FX-Rates
        url = cs.FXRATESURL
        try:
            html = gethtml(url)
            ut.writedatabase(cs.FXRATES, ('', todaydate, url, html), dbparameters)
            print(f'FX-Rates scraped and added to table {cs.FXRATES}. URL: {url}')
        except:
            html = ""
            print(f'FX-Rates scraping failure. URL: {url}') 
    else:
        scrapecompaniesfromreuters(shorts, dbparameters, 'fin')
        scrapecompaniesfromreuters(shorts, dbparameters, 'mar')
            
    print(f'Scraping procedure terminated: {totalno-len(errorlist)}/{totalno} successfully scraped')
    print('Failed Companies:')
    print(errorlist)


def getdata(datatype):      
    #Prepare Database to store data in
    dbparameters = ut.opendatabase(cs.RAWDATAPATH)
    
    if datatype == 'fin':
        ut.createtables(list(cs.ADDURLS_TO_TABLENAMES.values()), dbparameters)
    elif datatype == 'mar':
        ut.createtables([cs.STOCKDATA], dbparameters)
        ut.createtables([cs.FXRATES], dbparameters)
    else:
        ut.createtables(list(cs.ADDURLS_TO_TABLENAMES.values()), dbparameters)
        ut.createtables([cs.STOCKDATA], dbparameters)
        ut.createtables([cs.FXRATES], dbparameters)
    
    #Read RICs
    RICs = read_rics_from_csv(cs.RICSCSVPATH)
    
    #Get data for RICs from Reuters and store them in Database
    scrapecompaniesfromreuters(RICs, dbparameters, datatype)
    
    #Close Database
    ut.closedatabase(dbparameters)

print('Which data shall be scraped?\n-Financials (fin) \n-Marketdata (mar) \n-All (all)')
datatype = input()
if datatype in ['fin', 'mar', 'all']:
    getdata(datatype)
else:
    print('Error: Invalid Parameter')
      