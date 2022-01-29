#товары повседневного спроса

import pandas as pd
import yfinance as yf
import numpy as np
import redis
import json

staples_moex = pd.read_html('https://smart-lab.ru/q/shares/order_by_last_to_year_price/desc/?sector_id%5B%5D=5')[0]
staples_moex.drop(staples_moex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19]], axis = 1, inplace = True)
staples_moex["ytd, %"] = [f'{el}'.replace('+', '').strip()  for el in staples_moex["ytd, %"]]
staples_moex["ytd, %"] = [f'{el}'.replace('%', '').strip()  for el in staples_moex["ytd, %"]]
staples_moex["ytd, %"] = [f'{el}'.replace('nan', '-100').strip()  for el in staples_moex["ytd, %"]]#УСЛОВНО
staples_moex["ytd, %"] = staples_moex["ytd, %"].astype(float)
staples_moex = staples_moex.loc[staples_moex["ytd, %"] >= - 30]
staples_moex["Название"] = [f'{el}'.split(' ')[0]  for el in staples_moex["Название"]]
staples_moex["Название"] = [f'{el}'.split('-')[0]  for el in staples_moex["Название"]]
staples_moex["Название"] = [f'{el}'.replace('ао', '').strip()  for el in staples_moex["Название"]]
staples_moex["Название"] = [f'{el}'.replace('.', '').strip()  for el in staples_moex["Название"]]
staples_moex["Тикер"] = [f'{el}.ME' for el in staples_moex["Тикер"]]

staples_spbex = pd.read_html('https://smart-lab.ru/q/spbex/order_by_article_count_comment/desc/?sector_id%5B%5D=104&country_id=1')[0]
staples_spbex.drop(staples_spbex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14]], axis = 1, inplace = True)
staples_spbex = staples_spbex.loc[staples_spbex["Активность"] >= 20]

staples = pd.concat([staples_moex, staples_spbex])
staples.drop(staples.columns[[0]], axis = 1, inplace = True)
staples_list_company_name = staples["Название"].tolist()
staples_list_tic = staples["Тикер"].tolist()
staples_dict = dict(zip(staples_list_company_name, staples_list_tic))

conn = redis.Redis('localhost')

print(json.dumps(staples_dict))
conn.hmset("staples_dict", staples_dict)
