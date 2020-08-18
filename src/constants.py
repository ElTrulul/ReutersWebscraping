#URLs
ROOTURL = 'https://www.reuters.com/companies/'
FXRATESURL = 'https://www.reuters.com/markets/currencies'

#ADDURLs
INCSTAT_ANN_URL = '/financials/income-statement-annual/'
INCSTAT_QRT_URL = '/financials/income-statement-quarterly/'
BS_ANN_URL = '/financials/balance-sheet-annual/'
BS_QRT_URL = '/financials/balance-sheet-quarterly/'
KEYMETRICS_URL = '/key-metrics/'


#TABLENAMES
STOCKDATA = 'stockdata'
FXRATES = 'fxrates'
INCSTAT_ANN = 'incstat_ann'
INCSTAT_QRT = 'incstat_qrt'
BS_ANN = 'bs_ann'
BS_QRT = 'bs_qrt'
KEYMETRICS = 'km'

#TIMES
YEARS = ['2015', '2016', '2017', '2018', '2019']
QRTS = ['2019Q2', '2019Q3', '2019Q4', '2020Q1', '2020Q2']

#DICTIONARIES
ADDURLS_TO_TABLENAMES = {
            INCSTAT_ANN_URL: INCSTAT_ANN,\
            INCSTAT_QRT_URL:INCSTAT_QRT,\
            BS_ANN_URL: BS_ANN,\
            BS_QRT_URL: BS_QRT, \
            KEYMETRICS_URL: KEYMETRICS}
TABLENAMES_TO_DATA = {
            INCSTAT_ANN: {
                                'Total Revenue' :[0, 'int64'],\
                                'Net Income' : [0, 'int64']},\
            INCSTAT_QRT:{
                                'Total Revenue': [0, 'int64'],\
                                'Net Income': [0, 'int64']},\
            BS_ANN: {
                                'Total Equity' : [0, 'int64'],\
                                'Total Liabilities' : [0, 'int64']},\
            BS_QRT: {
                                'Total Equity' : [0, 'int64'],\
                                'Total Liabilities' : [0, 'int64']}, \
            KEYMETRICS: {
                                'Dividend (Per Share Annual)' : [0, 'float64' ],\
                                'Free Cash Flow (Per Share TTM)' : [0, 'float64'],\
                                'Current Ratio (Annual)' : [0, 'float64']}
           }
FACTORS = {
    'Mil': 1000000,
    'Thousands': 1000}
COLUMNHEADERSDICT_ANN = {
                    "Unnamed: 0":"Item", \
                    "Unnamed: 1":YEARS[-1],\
                    "Unnamed: 2":YEARS[-2],\
                    "Unnamed: 3":YEARS[-3],\
                    "Unnamed: 4":YEARS[-4],\
                    "Unnamed: 5":YEARS[-5], \
                    0:"Item", \
                    1: YEARS[-1]} # Unnamed for financials - 0,1 for non-financials
COLUMNHEADERSDICT_QRT = {
                    "Unnamed: 0":"Item", \
                    "Unnamed: 1":QRTS[-1],\
                    "Unnamed: 2":QRTS[-2],\
                    "Unnamed: 3":QRTS[-3],\
                    "Unnamed: 4":QRTS[-4],\
                    "Unnamed: 5":QRTS[-5], \
                    0:"Item", \
                    1: YEARS[-1]} # Unnamed for financials - 0,1 for non-financials
ISIN_TO_COUNTRIES = {
    'US' : 'USA',
    'DE' : 'Germany',
    'GB' : 'UK',
    'NL' : 'Netherlands',
    'IE' : 'Ireland',
    'FR' : 'France',
    'CA' : 'Canada',
    'CH' : 'Switzerland'
}

#PATHS
RICSCSVPATH = "..\\data\\01_raw\\reuters-shorts.csv"
RAWDATAPATH = "..\\data\\01_raw\\rawdatadb.db"
INTDATAPATH = "..\\data\\02_intermediate\\intdatadb.db"
PROCDATAPATH = '..\\data\\03_processed\\processeddata.feather'
PROCDATAPATHCSV = '..\\data\\03_processed\\processeddata.csv'

CURRENCIES = ['USD', 'EUR', 'GBP','CHF', 'INR']