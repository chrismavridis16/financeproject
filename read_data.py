import pandas as pd
import sqlite3

# Read sqlite query results into a pandas DataFrame
con = sqlite3.connect("data.sqlite")
df = pd.read_sql_query("SELECT * from DATA", con)

# Verify that result of SQL query is stored in the dataframe
print(df.to_string())

df = pd.read_sql_query("SELECT * from DATA", con)
data= pd.read_csv("C:/Users/xrism/Downloads/financeproject.csv", usecols = ['Symbol'])
for symbol in data['Symbol']:
    stock= df[df['SYMBOL']==symbol]
    #computed value, #average over 100 days, #average over 200 days
    for i in stock.index[50:]:
        stock_50 = stock.loc[i-50:i]['LTP']
        stock_100 = stock.loc[i-100:i]['LTP']
        stock_200 = stock.loc[i-200:i]['LTP']
        current_date = stock.loc[i]['DATE']
        current_symbol = symbol
        update = f"UPDATE DATA SET Computed_value = {stock_50.mean()} WHERE SYMBOL='{current_symbol}' and DATE='{current_date}' and D100='{stock_100.mean()}' and D200='{stock_200.mean()}';"
        print(update)
        cursor= con.execute(update)

con.commit()
con.close()
