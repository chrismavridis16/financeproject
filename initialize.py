import sqlite3
# importing the module
import pandas
import pandas as pd
from datetime import date
from nsepy import get_history
# read specific columns of csv file using Pandas


def market_data_update(f):
    df = pd.read_csv(f, usecols = ['Symbol'])
    print(df)
    data = pd.DataFrame()
    conn = sqlite3.connect('data.sqlite')
    for symbol in df['Symbol']:
        data = get_history(symbol= symbol, start=date(2022,8,22), end=date(2022,8,25))
        for i in data.index:
            # print(data)
            current_high = data.loc[i]['High']
            current_low = data.loc[i]['Low']
            current_mid = float(current_high - current_low) / 2
            current_symbol = data.loc[i]['Symbol']
            current_last = data.loc[i]['Last']
            current_date = i
            #conn.execute  (f'SELECT * FROM DATA WHERE DATE= {current_date} and symbol = {current_symbol}')
           # cursor_s= conn.execute(f"SELECT * FROM DATA WHERE DATE= '{current_date}' and symbol = '{current_symbol}';")
            select = f"SELECT * FROM DATA WHERE DATE='{current_date}' and SYMBOL='{current_symbol}';"
            print(select)
            cursor_s = conn.execute(select)

            resualt_count = len(cursor_s.fetchall())
            if resualt_count ==0:
                insert = f"INSERT INTO DATA (Symbol, High, Low, LTP, DATE, MID ) VALUES ('{current_symbol}', '{current_high}', '{current_low}', '{current_last}', '{current_date}', '{current_mid}');"
                #print(insert)
                column_mid = current_mid
                column_date = i
                column_symbol = current_symbol
                column_high = current_high
                column_low = current_low
                column_LTP = current_last
                df['MID'] = column_mid
                df['LOW'] = column_low
                df['LTP'] = column_LTP
                df['HIGH'] = column_high
                df['Symbol'] = column_symbol
                df['DATE'] = column_date
                cursor = conn.execute(insert)
            else:
                update = f"UPDATE DATA SET MID ='{current_mid}' WHERE SYMBOL='{current_symbol}' and DATE='{current_date}';"
                print(update)
                cursor = conn.execute(update)
            #print(df)

    conn.commit()
    print ("Records created successfully")
    conn.close()