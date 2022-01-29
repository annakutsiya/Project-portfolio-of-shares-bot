#товары вторичной необходимости

import pandas as pd
import yfinance as yf
import numpy as np
import redis
import json

discretionary_moex = pd.read_html('https://smart-lab.ru/q/shares/order_by_last_to_year_price/desc/?sector_id%5B%5D=13')[0]
discretionary_moex.drop(discretionary_moex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19]], axis = 1, inplace = True)
discretionary_moex["ytd, %"] = [f'{el}'.replace('+', '').strip()  for el in discretionary_moex["ytd, %"]]
discretionary_moex["ytd, %"] = [f'{el}'.replace('%', '').strip()  for el in discretionary_moex["ytd, %"]]
discretionary_moex["ytd, %"] = [f'{el}'.replace('nan', '-100').strip()  for el in discretionary_moex["ytd, %"]]#УСЛОВНО
discretionary_moex["ytd, %"] = discretionary_moex["ytd, %"].astype(float)
discretionary_moex = discretionary_moex.loc[discretionary_moex["ytd, %"] >= - 30]
discretionary_moex["Название"] = [f'{el}'.split(' ')[0]  for el in discretionary_moex["Название"]]
discretionary_moex["Название"] = [f'{el}'.split('-')[0]  for el in discretionary_moex["Название"]]
discretionary_moex["Название"] = [f'{el}'.replace('ао', '').strip()  for el in discretionary_moex["Название"]]
discretionary_moex["Название"] = [f'{el}'.replace('.', '').strip()  for el in discretionary_moex["Название"]]
discretionary_moex["Название"] = [f'{el}'.replace('i', '').strip()  for el in discretionary_moex["Название"]]
discretionary_moex["Тикер"] = [f'{el}.ME' for el in discretionary_moex["Тикер"]]

discretionary_spbex = pd.read_html('https://smart-lab.ru/q/spbex/order_by_article_count_comment/desc/?sector_id%5B%5D=103&country_id=1')[0]
discretionary_spbex.drop(discretionary_spbex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14]], axis = 1, inplace = True)
discretionary_spbex = discretionary_spbex.loc[discretionary_spbex["Активность"] >= 30]

discretionary = pd.concat([discretionary_moex, discretionary_spbex])
discretionary.drop(discretionary.columns[[0]], axis = 1, inplace = True)
discretionary_list_company_name = discretionary["Название"].tolist()
discretionary_list_tic = discretionary["Тикер"].tolist()
discretionary_dict = dict(zip(discretionary_list_company_name, discretionary_list_tic))

conn = redis.Redis('localhost')

print(json.dumps(discretionary_dict))
conn.hmset("discretionary_dict", discretionary_dict)
