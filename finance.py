#финансы

import pandas as pd
import yfinance as yf
import numpy as np
import redis
import json

fin_moex = pd.read_html('https://smart-lab.ru/q/shares/order_by_last_to_year_price/desc/?sector_id%5B%5D=2&sector_id%5B%5D=14')[0]
fin_moex.drop(fin_moex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19]], axis = 1, inplace = True)
fin_moex["ytd, %"] = [f'{el}'.replace('+', '').strip()  for el in fin_moex["ytd, %"]]
fin_moex["ytd, %"] = [f'{el}'.replace('%', '').strip()  for el in fin_moex["ytd, %"]]
fin_moex["ytd, %"] = [f'{el}'.replace('nan', '-100').strip()  for el in fin_moex["ytd, %"]]#УСЛОВНО
fin_moex["ytd, %"] = fin_moex["ytd, %"].astype(float)
fin_moex = fin_moex.loc[fin_moex["ytd, %"] >= -20]
fin_moex["Название"] = [f'{el}'.replace('ао', '').strip()  for el in fin_moex["Название"]]
fin_moex["Название"] = [f'{el}'.replace('ап', '').strip()  for el in fin_moex["Название"]]
fin_moex["Название"] = [f'{el}'.replace('Авангрд-', 'Авангрд').strip()  for el in fin_moex["Название"]]
fin_moex["Тикер"] = [f'{el}.ME' for el in fin_moex["Тикер"]]


fin_spbex = pd.read_html('https://smart-lab.ru/q/spbex/order_by_article_count_comment/desc/?sector_id%5B%5D=101&country_id=1')[0]
fin_spbex.drop(fin_spbex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14]], axis = 1, inplace = True)
fin_spbex = fin_spbex.loc[fin_spbex["Активность"] >= 30]

finance = pd.concat([fin_moex, fin_spbex])
finance.drop(finance.columns[[0]], axis = 1, inplace = True)
finance_list_company_name = finance["Название"].tolist()
finance_list_tic = finance["Тикер"].tolist()
finance_dict = dict(zip(finance_list_company_name, finance_list_tic))

conn = redis.Redis('localhost')

print(json.dumps(finance_dict))
conn.hmset("finance_dict", finance_dict)
