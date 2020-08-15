import constants as cs
import d00_utils as ut

from bs4 import BeautifulSoup
import pandas as pd
import time
import sqlite3
     

def extracttablefromsoup(soup, tableno):
    table = soup.find_all('table')[tableno]
    return pd.read_html(str(table), parse_dates=True)[0]


def formatnumber(strnumber,factor, intdigits):
    if type(strnumber) != "object":
        strnumber = str(strnumber)
    number = float(strnumber.replace("--","0.00").replace(")","").replace("(","-").replace(",", ""))
    return '{:.{}f}'.format(number*factor, intdigits)


def getdescription(soup, isfinancials):
    if isfinancials:
        result = soup.find("h3", string='Statements').next_sibling.get_text()   
    else:
        result = soup.find("h3", string='Per Share Data').next_sibling.get_text()
    return result


def getcompanyname(soup):
    return soup.find('div', class_='QuoteRibbon-name-ric-epp2J').h1.get_text()


def searchlistiteminstring(somelist, somestring):
    matches = [s for s in somelist if s in somestring]
    if len(matches)>1:
        print('Warning: There are several matches: ', matches)
        result = matches[0]
    elif len(matches) == 0:
        result = ""
    else:
        result = matches[0]
    return result


#def checkifallelementsareidentical(somelist):
#    return somelist.count(somelist[0]) == len(somelist)


def returnitemsofliststartingwith(somelist, somestring):
    return [i for i in somelist if somestring in i]


def extractrelevantdatafromhtml(table, pandasseries):
    #just for financial data, not market data!
    
    listdf = []
    scrcurrencies = []
    
    #basics
    isfinancials = False if table == cs.KEYMETRICS else True
    isquarterly = True if 'qrt' in table else False
    tableno = 0 if isfinancials else 1
    
    #scraping
    soup = BeautifulSoup(pandasseries.html, 'lxml')
    description = getdescription(soup, isfinancials)
    
    #data structuring
    df = extracttablefromsoup(soup, tableno)
    df = df[df.iloc[:,0].isin(cs.TABLENAMES_TO_DATA[table])]
    df = df.iloc[:,:-1] if isfinancials else df #drop last column (trend) if financials
    df = df.rename(columns = cs.COLUMNHEADERSDICT_QRT) if isquarterly else df.rename(columns = cs.COLUMNHEADERSDICT_ANN)
    df = df.set_index('Item')
    df = df.stack().to_frame().T
    df.columns = df.columns.map(' | '.join)
    
    #number formatting and adjusting dtypes
    factor = cs.FACTORS.get(searchlistiteminstring(cs.FACTORS, description),1)
    if isfinancials:
        df = df.applymap(lambda x: formatnumber(x, factor, 0))
        df = df.astype('int64')
    else:
        df = df.applymap(lambda x: formatnumber(x, factor, 2))
        df = df.astype('float')
    
    #append data
    listdf.append(df)
    
    #concatenate list and define index
    resultdf = pd.concat(listdf, axis=1)
    resultdf['RIC']= pandasseries.ric
    resultdf = resultdf.set_index('RIC')
    
    #currency
    resultdf['Currency'] = searchlistiteminstring(cs.CURRENCIES, description)
    
    #name
    resultdf['Name'] = getcompanyname(soup)
    
    #website
    resultdf['Website'] = pandasseries.website
    
    #timestamp
    resultdf['Timestamp'] = pandasseries.timestamp
    
    return resultdf


def extractandcleandatafinancials(rawcon, strcon):
    todaydate = int(time.time())
    errorlist = []
    for table in cs.TABLENAMES_TO_DATA:
        print(f'\nTable {table}: Process started.')
        dftable = ut.sqlquery(rawcon, table)
        print(f'Table {table}: Data retrieved.')
        dflist = []
        df = pd.DataFrame()
        for i in range(len(dftable)):
            RIC = dftable.loc[i, "ric"]
            try:
                dflist.append(extractrelevantdatafromhtml(table,dftable.loc[i, :]))
                print(f'Table {table}: {i+1}/{len(dftable)} Company {RIC} successfully converted.')
            except:
                errorlist.append(RIC + ' in ' + table)
                print(f'Table {table}: {i+1}/{len(dftable)} Company {RIC} failed!')  
        print(f'\nTable {table}: Relevant information extracted.')
        df = pd.concat(dflist, sort=True)
        print(f'Table {table}: Relevant information concatenated.')
        for item in cs.TABLENAMES_TO_DATA[table]:
            df[returnitemsofliststartingwith(df.columns, item)] = df[returnitemsofliststartingwith(df.columns, item)].fillna(cs.TABLENAMES_TO_DATA[table][item][0])
            df[returnitemsofliststartingwith(df.columns, item)] = df[returnitemsofliststartingwith(df.columns, item)].astype(cs.TABLENAMES_TO_DATA[table][item][1])
        print(f'Table {table}: Data cleaned.')
        df.to_sql(table, con=strcon, if_exists='replace')
        print(f'Table {table}: Data exported to database.')
    print('\nExtraction and cleaning procedure successfully terminated.')
    print('Failed Companies:')
    print(errorlist)


def extractandcleandatamarket(rawcon, strcon):
    #STOCKDATA
    todaydate = int(time.time())
    dflist = []
    errorlist = []
    print(f'\nTable {cs.STOCKDATA}: Process started.')
    dftable = ut.sqlquery(rawcon, cs.STOCKDATA)
    print(f'Table {cs.STOCKDATA}: Data retrieved.')
    
    #Extract relevant data from html
    for i in range(len(dftable)):
        pandasseries = dftable.loc[i,:]
        RIC = pandasseries.ric
        try:
            soup = BeautifulSoup(pandasseries.html, 'lxml')
            table = soup.find_all('table')[0]
            dfmarketdata = pd.read_html(str(table), parse_dates=True)[0].T
            dfmarketdata.columns = dfmarketdata.iloc[0]
            dfmarketdata = dfmarketdata.drop(dfmarketdata.index[0])
            dfmarketdata['Latest Trade'] = soup.find('p', string='Latest Trade').next_sibling.get_text()
            dfmarketdata['Currency'] = soup.find('p', string='Latest Trade').next_sibling.next_sibling.get_text()
            dfmarketdata['RIC'] = RIC
            dfmarketdata['Timestamp'] = pandasseries.timestamp
            dfmarketdata['Website'] = pandasseries.website
            dfmarketdata = dfmarketdata.set_index('RIC')
            dflist.append(dfmarketdata)
            print(f'Table {cs.STOCKDATA}: {i+1}/{len(dftable)} Company {RIC} successfully converted.')
        except:
            errorlist.append(RIC + ' in ' + cs.STOCKDATA)
            print(f'Table {cs.STOCKDATA}: {i+1}/{len(dftable)} Company {RIC} failed!')  
    print(f'\nTable {cs.STOCKDATA}: Relevant information extracted.')
    dfmarketdata = pd.concat(dflist, sort=True)
    print(f'Table {cs.STOCKDATA}: Relevant information concatenated.')
    
    #Data Cleaning and Formatting
    dfmarketdata = dfmarketdata[['52 Week High', 
                                 '52 Week Low',
                                 'Previous Close', 
                                 'Open',
                                 "Today's High", 
                                 "Today's Low", 
                                 'Latest Trade',
                                 'Currency', 
                                 'Dividend (Yield %)', 
                                 'Shares Out (MIL)', 
                                 'Market Cap (MIL)', 
                                 'Volume', 
                                 '3M AVG Volume',
                                 'Forward P/E',
                                 'Timestamp',
                                 'Website']]
    dfmarketdata.iloc[:,:7] = dfmarketdata.iloc[:,:7].applymap(lambda x : formatnumber(x,1,2))
    dfmarketdata.iloc[:,8] = dfmarketdata.iloc[:,8].apply(lambda x : formatnumber(x,1/100,4))
    dfmarketdata.iloc[:,9:11] = dfmarketdata.iloc[:,9:11].applymap(lambda x : formatnumber(x,1000000,0))
    dfmarketdata.iloc[:,11] = dfmarketdata.iloc[:,11].apply(lambda x : formatnumber(x,1,0))
    dfmarketdata.iloc[:,12:14] = dfmarketdata.iloc[:,12:14].applymap(lambda x : formatnumber(x,1,2))
    dfmarketdata = dfmarketdata.astype({
     '52 Week High': 'float64', 
     '52 Week Low': 'float64',
     'Previous Close': 'float64', 
     'Open': 'float64',
     "Today's High": 'float64', 
     "Today's Low": 'float64', 
     'Latest Trade': 'float64',
     'Dividend (Yield %)': 'float64', 
     'Shares Out (MIL)': 'int64', 
     'Market Cap (MIL)': 'int64', 
     'Volume': 'int64', 
     '3M AVG Volume': 'float64',
     'Forward P/E': 'float64',
     'Timestamp': 'int64'  
    })
    print(f'Table {cs.STOCKDATA}: Data cleaned.')
    dfmarketdata.to_sql(cs.STOCKDATA, con=strcon, if_exists='replace')
    print(f'Table {cs.STOCKDATA}: Data exported to database.')
    
    #FX-RATES
    print(f'\nTable {cs.FXRATES}: Process started.')
    dftable = ut.sqlquery(rawcon, "fxrates")
    print(f'Table {cs.FXRATES}: Data retrieved.')
    pandasseries = dftable.loc[0,:]
    soup = BeautifulSoup(pandasseries.html, 'html.parser')
    table = soup.find_all('table')[0]
    dffxrates = pd.read_html(str(table), parse_dates=True)[0][['Currency', 'Last']]
    dffxrates = dffxrates.set_index('Currency')  
    print(f'Table {cs.FXRATES}: Relevant information extracted.')
    dffxrates.to_sql("fxrates", con=strcon, if_exists='replace')
    print(f'Table {cs.FXRATES}: Data exported to database.')
    print('\nExtraction and cleaning procedure successfully terminated.')
    print('Failed Companies:')
    print(errorlist)
    
def preprocessdata(datatype='market'):
    #Open databases to read and write data
    dbread = ut.opendatabase(cs.RAWDATAPATH)
    dbwrite = ut.opendatabase(cs.INTDATAPATH)
    
    #Extract and clean data
    if datatype == 'fin':
        extractandcleandatafinancials(dbread[0], dbwrite[0])
    elif datatype == 'mar':
        extractandcleandatamarket(dbread[0], dbwrite[0])
    else:
        extractandcleandatafinancials(dbread[0], dbwrite[0])
        extractandcleandatamarket(dbread[0], dbwrite[0])
        
    #Close databases
    ut.closedatabase(dbread)
    ut.closedatabase(dbwrite)

print('Which data shall be preprocessed?\n-Financials (fin) \n-Marketdata (mar) \n-All (all)')
datatype = input()
if datatype in ['fin', 'mar', 'all']:
    preprocessdata(datatype)
else:
    print('Error: Invalid Parameter')