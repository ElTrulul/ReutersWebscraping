import constants as cs
import d00_utils as ut

import pandas as pd
import time
import sqlite3


def normalizecurrencyonusd(dataframe, fxrates):
    dataframe.select_dtypes(exclude=['object', 'bool'])
    dataframe['Currency'] = dataframe['Currency'].map(lambda x: x.upper())
    fxratescolumns = dataframe['Currency'].map(fxrates)
    if fxratescolumns.isna().sum()>0:
        print('Warning: Some unknown currency is used and causing trouble....')
    for column in dataframe.select_dtypes(exclude=['object', 'bool']).columns:
        dataframe[column] *= fxratescolumns
    return dataframe.drop('Currency', axis=1)


def growthscore(row, category, strtype='years'):
    score = 0
    if strtype == 'years':
        for i in range(len(cs.YEARS)-1):
            score = score + 1 if row[category + ' | ' + cs.YEARS[i+1]] > row[category + ' | ' + cs.YEARS[i]] else score
    elif strtype == 'qrts':
        for i in range(len(cs.QRTS)-1):
            score = score + 1 if row[category + ' | ' + cs.QRTS[i+1]] > row[category + ' | ' + cs.QRTS[i]] else score
    else:
        print('Error: Unknown argument strtype.')
    return score


def cleanandmergealldata(strcon):    
    #Read data in dataframes
    dfricsdata = pd.read_csv(cs.RICSCSVPATH, index_col = "RIC") 
    dfbsann = ut.sqlquery(strcon, cs.BS_ANN).set_index('RIC') 
    dfbsqrt = ut.sqlquery(strcon, cs.BS_QRT).set_index('RIC')
    dfisann = ut.sqlquery(strcon, cs.INCSTAT_ANN).set_index('RIC') 
    dfisqrt = ut.sqlquery(strcon, cs.INCSTAT_QRT).set_index('RIC') 
    dfstockdata = ut.sqlquery(strcon, cs.STOCKDATA).set_index('RIC') 
    dfkeymetrics = ut.sqlquery(strcon, cs.KEYMETRICS).set_index('RIC') 
    dffxrates = ut.sqlquery(strcon, cs.FXRATES).set_index('Currency') 
    
    #Convert fxrates to dictionary
    fxrates = dffxrates.to_dict()['Last']
    fxrates['USD'] = 1.000
    
    #Clean RICs Data
    dfricsdata = dfricsdata.fillna({'NoOwnedShares' : 0,
                                      'BuyingPricePerShare' : 0,
                                      'Currency': 'USD',
                                      'IsOwned' : 'Nein',
                                      'Sector' : '',
                                      'ISIN': ''})
    dfricsdata['IsOwned'] = dfricsdata['IsOwned'].map({'Ja' : True, 'Nein' : False})
    dfricsdata = dfricsdata.astype({'NoOwnedShares': 'int64', 'IsOwned' : 'bool', 'Currency' : 'object'})
    
    #Normalize Currencies to USD -> dffxrates is not needed from here
    noshares = dfricsdata['NoOwnedShares']
    dfricsdata = normalizecurrencyonusd(dfricsdata, fxrates)
    dfricsdata['NoOwnedShares'] = noshares
    dfbsann = normalizecurrencyonusd(dfbsann, fxrates)
    dfbsqrt = normalizecurrencyonusd(dfbsqrt, fxrates)
    dfisann = normalizecurrencyonusd(dfisann, fxrates)
    dfisqrt = normalizecurrencyonusd(dfisqrt, fxrates)
    dfstockdata = normalizecurrencyonusd(dfstockdata, fxrates)
    dfkeymetrics = normalizecurrencyonusd(dfkeymetrics, fxrates)
    
    #Get Country from ISIN
    dfricsdata['Country'] = dfricsdata['ISIN'].apply(lambda x: x[:2]).map(cs.ISIN_TO_COUNTRIES)
    dfricsdata['Country'] = dfricsdata['Country'].fillna('')
    
    #Merge all Data to one dataframe
    dfmain = pd.concat([dfricsdata, dfbsann, dfbsqrt, dfisann, dfisqrt, dfstockdata,dfkeymetrics], axis=1, sort=False)
    dfmain = dfmain.loc[:,~dfmain.columns.duplicated()]
    
    return dfmain


def growthrate(columnnew, columnold):
    return (columnnew - columnold)/columnold.apply(lambda x: abs(x))


def calculatekpis(dfmain):
    years = cs.YEARS
    qrts = cs.QRTS
    
    #GrowthRates
    for i in range(len(years)-1):
        dfmain['NetIncome Growth '+years[i+1]] = growthrate(dfmain['Net Income | '+years[i+1]], dfmain['Net Income | '+years[i]])
        dfmain['NetIncome Growth '+qrts[i+1]] = growthrate(dfmain['Net Income | '+qrts[i+1]], dfmain['Net Income | '+qrts[i]])
        dfmain['Total Revenue Growth '+years[i+1]] = growthrate(dfmain['Total Revenue | '+years[i+1]], dfmain['Total Revenue | '+years[i]])
        dfmain['Total Revenue Growth '+qrts[i+1]] = growthrate(dfmain['Total Revenue | '+qrts[i+1]], dfmain['Total Revenue | '+qrts[i]])
        
    #KPIs
    dfmain['P/E ' + years[-1]] = dfmain['Market Cap (MIL)']/dfmain['Net Income | ' + years[-1]]
    dfmain['Dividendenrendite'] = dfmain['Dividend (Per Share Annual) | '+ years[-1]]/dfmain["Latest Trade"]
    dfmain['Verschuldungsgrad '+ years[-1]] = dfmain['Total Liabilities | ' +  years[-1]]/dfmain['Total Equity | '+ years[-1]]
    dfmain['Verschuldungsgrad '+ qrts[-1]] = dfmain['Total Liabilities | '+ qrts[-1]]/dfmain['Total Equity | '+ qrts[-1]]
    dfmain['Potential'] = (dfmain["Latest Trade"] - dfmain['52 Week Low'])/(dfmain['52 Week High'] - dfmain['52 Week Low'])
    dfmain['Return on Sales '+ years[-1]] = dfmain['Net Income | '+ years[-1]]/dfmain['Total Revenue | '+ years[-1]]
    dfmain['Return on Sales '+ qrts[-1]] = dfmain['Net Income | '+ qrts[-1]]/dfmain['Total Revenue | '+ qrts[-1]]
    dfmain['CurrentInvestment'] = dfmain['NoOwnedShares']*dfmain['Latest Trade']
    dfmain['CurrentProfit'] = (dfmain['Latest Trade']-dfmain['BuyingPricePerShare'])*dfmain['NoOwnedShares']
    
    #GrowthScores
    dfmain['Score-TotalRevenue-years']=dfmain.apply(lambda row: growthscore(row, 'Total Revenue', 'years'), axis=1)
    dfmain['Score-TotalRevenue-qrts']=dfmain.apply(lambda row: growthscore(row, 'Total Revenue', 'qrts'), axis=1)
    dfmain['Score-NetIncome-years']=dfmain.apply(lambda row: growthscore(row, 'Net Income', 'years'), axis=1)
    dfmain['Score-NetIncome-qrts']=dfmain.apply(lambda row: growthscore(row, 'Net Income', 'qrts'), axis=1)
    
    return dfmain.sort_values(by=['Sector','Forward P/E'])
    

def processingdata():
    #Open database to read and write data
    dbread = ut.opendatabase(cs.INTDATAPATH)
    
    df = cleanandmergealldata(dbread[0])
    df = calculatekpis(df)
    
    ut.writecsv(df, cs.PROCDATAPATHCSV)
    ut.writefeather(df, cs.PROCDATAPATH)
    ut.closedatabase(dbread)
    
print('Start processing data...')
processingdata()
print('Process successfully terminated')
    