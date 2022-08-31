import pandas as pd
import sqlite3
from datetime import date
from nsepy import get_history
import talib
from talib import ADX
from talib import RSI

def historical_update(f):
    # Read sqlite query results into a pandas DataFrame
    con = sqlite3.connect("data.sqlite")
    df = pd.read_sql_query("SELECT * from DATA", con)

    # Verify that result of SQL query is stored in the dataframe
   # print(df.to_string())

    df = pd.read_sql_query("SELECT * from DATA", con)
    data = pd.read_csv(f, usecols = ['Symbol'])
    for symbol in data['Symbol']:
        stock = df[df['SYMBOL']==symbol]
        #computed value, #average over 100 days, #average over 200 days
        l = len(stock.index)
        for i in stock.index[50:l]:
            stock_50 = stock.loc[i-50:i]['LTP']
            stock_100 = stock.loc[i-100:i]['LTP']
            stock_200 = stock.loc[i-200:i]['LTP']
            stock_high = stock.loc[i-199:i]['HIGH']
            stock_low = stock.loc[i-199:i]['LOW']
            stock_close = stock.loc[i-199:i]['LTP']
            real = ADX(stock_high.to_numpy(), stock_low.to_numpy(), stock_close.to_numpy(), timeperiod=14)
            current_date = stock.loc[i]['DATE']
            current_symbol = symbol
            update = f"UPDATE DATA SET Computed_value ='{stock_50.mean()}' , D100='{stock_100.mean()}' , D200='{stock_200.mean()}' , ADX='{real[-1]}' WHERE SYMBOL='{current_symbol}' and DATE='{current_date}';"
            print(update)

            cursor = con.execute(update)
            con.commit()


    con.close()
    return


def daily_update(f, current_date):
    con = sqlite3.connect("data.sqlite")
    df = pd.read_sql_query("SELECT * from DATA;", con)

    # Verify that result of SQL query is stored in the dataframe
    print(df.to_string())

    data = pd.read_csv(f, usecols=['Symbol'])
    for symbol in data['Symbol']:
        print(symbol)
        stock = df[df['SYMBOL'] == symbol]
        stock.reset_index().drop(columns='index')
        i = len(stock)



        if i >=1:
            #insert = f"INSERT INTO DATA (Symbol, High, Low, LTP, DATE, MID, COMPUTED_VALUE, D50, D100, D200) VALUES ('{current_symbol}', '{current_high}', '{current_low}', '{current_last}', '{current_date}', '{current_mid}','{stock_50}', '{stock_100}', '{stock_200}');"
            #print(insert)
            stock_50 = stock.loc[i-49:i]['LTP']
            stock_100 = stock.loc[i-99:i]['LTP']
            stock_200 = stock.loc[i-199:i]['LTP']
            stock_high = stock.loc[i-199:i]['HIGH']
            stock_low = stock.loc[i-199:i]['LOW']
            stock_close= stock.loc[i-199:i]['LTP']

            #current_date = stock.loc[i-1]['DATE']
            current_symbol = symbol
            stock_today = get_history(symbol=symbol, start=current_date, end=current_date)
            if len(stock_today)>0:
                stock_today = stock_today.reset_index()
                stock_50 = pd.concat([stock_50, stock_today['Last']])
                stock_100 = pd.concat([stock_100, stock_today['Last']])
                stock_200 = pd.concat([stock_200, stock_today['Last']])
                stock_high = pd.concat([stock_high, stock_today['High']])
                stock_low = pd.concat([stock_low, stock_today['Low']])
                stock_close = pd.concat([stock_close, stock_today['Last']])
                #  print(stock_high.to_numpy())
                real = ADX(stock_high.to_numpy(), stock_low.to_numpy(), stock_close.to_numpy(), timeperiod=14)
                real2 = RSI(stock_close.to_numpy(), timeperiod=14)
                #print(real)
                print(real[-1])
                print(real[-2])
                #return
                #update = f"UPDATE DATA SET Computed_value = {stock_50.mean()} WHERE SYMBOL='{current_symbol}' and DATE='{current_date}' and D100='{stock_100.mean()}' and D200='{stock_200.mean()}';"
               #print(update)
                current_high = stock_today.loc[0]['High']
                current_low = stock_today.loc[0]['Low']
                current_mid = float(current_high - current_low) / 2
                #current_symbol = stock_today.loc[i]['Symbol']
                current_last = stock_today.loc[0]['Last']
                insert = f"INSERT INTO DATA (Symbol, High, Low, LTP, DATE, MID, COMPUTED_VALUE, D100, D200, ADX, RSI) VALUES ('{current_symbol}', '{current_high}', '{current_low}', '{current_last}', '{current_date}', '{current_mid}','{stock_50.mean()}', '{stock_100.mean()}', '{stock_200.mean()}', '{real[-1]}', '{real2[-1]}');"
                print(insert)
                cursor = con.execute(insert)
                con.commit()


