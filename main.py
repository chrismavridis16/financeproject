from initialize import market_data_update
from update_indicators import historical_update
from update_indicators import daily_update
from datetime import date
import datetime
file_name = "financeproject.csv"


def main():
    #market_data_update(file_name)
    historical_update(file_name)
    #daily_update(file_name, datetime.date.today())


if __name__ == "__main__":
    main()

