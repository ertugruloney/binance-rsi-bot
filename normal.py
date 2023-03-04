

import pandas as pd
import numpy as np
from binance.client import Client
from binance import ThreadedWebsocketManager
import datetime
import logging

import requests
import time

from urllib.parse import urlencode

import hmac
import hashlib
from BinanceClass import BinanceFuturesClient



from ta.trend import EMAIndicator
from ta.momentum import  RSIIndicator
api_key=""
secret_key=""
binance = BinanceFuturesClient(api_key,secret_key,False)
data=pd.read_excel("data.xlsx")

client=Client(api_key=api_key,api_secret=secret_key, tld="com")
def get_history(symbol, interval, start, end = None):
    bars = client.futures_historical_klines(symbol = symbol, interval = interval,
                                        start_str = start, end_str = end, limit = 1000)
    df = pd.DataFrame(bars)
    df["Date"] = pd.to_datetime(df.iloc[:,0], unit = "ms")
    df.columns = ["Open Time", "Open", "High", "Low", "Close", "Volume",
                  "Clos Time", "Quote Asset Volume", "Number of Trades",
                  "Taker Buy Base Asset Volume", "Taker Buy Quote Asset Volume", "Ignore", "Date"]
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
    df.set_index("Date", inplace = True)
    for column in df.columns:
        df[column] = pd.to_numeric(df[column], errors = "coerce")
    
    return df

data=data.values.tolist()



def qty(data):
    qts=[]
    
    for i in range(len(data)):
        a=float(client.get_symbol_ticker(symbol = data[i][0]+"USDT")["price"])
       
        qt=(data[i][9]/a)*data[i][1]
        qt=round(qt,3)
        if qt<1:
            qt=1
        else:
            qt=int(qt)
        qts.append(qt)
    return qts    
qts=qty(data)


#%% veri önişleme
def indakatorekleme(data):
    an = datetime.datetime.now()
    if an.day>3: 
        tarih=str(an.year) +"-"+ str(an.day-2)+"-"+str(an.month)
    else:
        
       tarih=str(an.year) +"-"+ str(29)+"-"+str(an.month-1) 
    veriler=[]
    dates=[]
    
    uzun=np.array(data).shape
    if len (uzun)>1:
        for i in data:
            name=i[0]
            name=name+"USDT"
            c=str(i[5])
            veri=get_history(symbol = name, interval = c, start =tarih)
            datees=veri.index
            datees=datees.values.tolist()
            datees=datees[-1]
            dates.append(datees)
            ema1=EMAIndicator(veri["Close"],window = i[6])
            ema2=EMAIndicator(veri["Close"],window = i[7])
            rsi=RSIIndicator(veri["Close"],window = i[8])
            veri["rsi"]=rsi.rsi()
            veri["ema1"]=ema1.ema_indicator()
            veri["ema2"]=ema2.ema_indicator()
            
            buyuk=i[6]
            if i[6]<i[7]:
                buyuk=i[7]
            veriler.append(veri[len(veri)-buyuk:]) 
    else:
            i=data
            name=i[0]
            name=name+"USDT"
            c=str(i[5])
            veri=get_history(symbol = name, interval = c, start = tarih)
            datees=veri.index
            datees=datees.values.tolist()
            datees=datees[-1]
            dates.append(datees)
            ema1=EMAIndicator(veri["Close"],window = i[6])
            ema2=EMAIndicator(veri["Close"],window = i[7])
            rsi=RSIIndicator(veri["Close"],window = i[8])
            veri["rsi"]=rsi.rsi()
            veri["ema1"]=ema1.ema_indicator()
            veri["ema2"]=ema2.ema_indicator()
            
            buyuk=i[6]
            if i[6]<i[7]:
                buyuk=i[7]
            veriler.append(veri[len(veri)-buyuk:]) 
    return  dates,  veriler
dates,veriler=   indakatorekleme(data) 
status=np.zeros(len(veriler)) # eğer 0 ise ema1 ema2' altındar


for count,i in enumerate(veriler):
    uzunluk=len(i["Open"])-1

    if i.iloc[uzunluk,6]>i.iloc[uzunluk,7]:
        status[count]=1

depotarihi=np.zeros(len(veriler))
signals=np.zeros(len(veriler))
for i in range(len(veriler)):
    depotarihi[i]=dates[i]
#%%  
countt=0
while True:
    dates,veriler=   indakatorekleme(data) 
    dff=pd.DataFrame(dates)
    dfs= pd.to_datetime(dff.iloc[:,0])  
    qts=qty(data)
    if countt==0:
        for count,i in enumerate(veriler):
            uzunluk=len(i["Open"])-1
            depo1=status[count]
            
            if i.iloc[uzunluk,6]>i.iloc[uzunluk,7]:
                status[count]=1
            else:
                status[count]=0
              
            if int(i.iloc[uzunluk,5])>55:
                        if status[count]==1:
                            
                            binance.leveragee(symbol=str(data[count][0])+"USDT",leverage=data[count][1])
                            time.sleep(1)
                            client.futures_create_order( symbol=str(data[count][0])+"USDT",side="BUY",quantity=qts[count] ,type="MARKET")
                            signals[count]=2
                            print( f'long işlemi açıldı".{str(data[count][0])} tarih { dfs[count]}')
            if   i.iloc[uzunluk,5]<55 :
                        if status[count]==0:
                            binance.leveragee(symbol=str(data[count][0])+"USDT",leverage=data[count][1])
                            time.sleep(1)
                            client.futures_create_order( symbol=str(data[count][0])+"USDT",side="SELL",quantity=qts[count] ,type="MARKET")
                            signals[count]=1
                            print( f'short işlemi açıldı".{str(data[count][0])} tarih { dfs[count]}')
    else:
            for count,i in enumerate(veriler):
                if depotarihi[count]!=dates[count]:
                    uzunluk=len(i["Open"])-1
                    depo1=status[count]
                   
                    if i.iloc[uzunluk,6]>i.iloc[uzunluk,7]:
                        status[count]=1
                    else:
                        status[count]=0
                       
                    if i.iloc[uzunluk,5]>55:
                            if status[count]==1:
                                if signals[count]==0:
                                    binance.leveragee(symbol=str(data[count][0])+"USDT",leverage=data[count][1])
                                 
                                    client.futures_create_order( symbol=str(data[count][0])+"USDT",side="BUY",quantity=qts[count] ,type="MARKET")
                                    signals[count]=2
                                    print( f'long işlemi açıldı".{str(data[count][0])} tarih { dfs[count]}')
                    if signals[count]==1:
                        if i.iloc[uzunluk,5]>55:
                            if status[count]==1:
                                    binance.leveragee(symbol=str(data[count][0])+"USDT",leverage=data[count][1])
                                    time.sleep(1)
                                    client.futures_create_order( symbol=str(data[count][0])+"USDT",side="BUY",quantity=qts[count],type="MARKET")
                                    
                                    
                                    time.sleep(1)
                                    client.futures_create_order( symbol=str(data[count][0])+"USDT",side="BUY",quantity=qts[count] ,type="MARKET")
                                    print( f'long işlemi açıldı".{str(data[count][0])} tarih { dfs[count]}')
                                    signals[count]=2
                    
                    if i.iloc[uzunluk,5]<55:
                            if status[count]==0:
                                if signals[count]==0:
                                    binance.leveragee(symbol=str(data[count][0])+"USDT",leverage=data[count][1])
                                    time.sleep(1)
                                    client.futures_create_order( symbol=str(data[count][0])+"USDT",side="SELL",quantity=qts[count] ,type="MARKET")
                                    signals[count]=1
                                    print( f'short işlemi açıldı".{str(data[count][0])} tarih { dfs[count]}')
                    if signals[count]==2:
                        if i.iloc[uzunluk,5]<55:
                            if status[count]==0:
                                    binance.leveragee(symbol=str(data[count][0])+"USDT",leverage=data[count][1])
                                    time.sleep(1)
                                    client.futures_create_order( symbol=str(data[count][0])+"USDT",side="SELL",quantity=qts[count] ,type="MARKET")
                                    time.sleep(1)
                                  
                                    client.futures_create_order( symbol=str(data[count][0])+"USDT",side="SELL",quantity=qts[count] ,type="MARKET")
                                    print( f'short işlemi açıldı".{str(data[count][0])} tarih { dfs[count]}')
                                    signals[count]=1
    for i in range(len(veriler)):
        depotarihi[i]=dates[i]
    countt=countt+1
    print(countt)