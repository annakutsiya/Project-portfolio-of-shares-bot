#техгологии

import pandas as pd
import yfinance as yf
import numpy as np
import redis
import json

technology_moex = pd.read_html('https://smart-lab.ru/q/shares/order_by_last_to_year_price/desc/?sector_id%5B%5D=15')[0]
technology_moex.drop(technology_moex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19]], axis = 1, inplace = True)
technology_moex["ytd, %"] = [f'{el}'.replace('+', '').strip()  for el in technology_moex["ytd, %"]]
technology_moex["ytd, %"] = [f'{el}'.replace('%', '').strip()  for el in technology_moex["ytd, %"]]
technology_moex["ytd, %"] = [f'{el}'.replace('nan', '-100').strip()  for el in technology_moex["ytd, %"]]#УСЛОВНО
technology_moex["ytd, %"] = technology_moex["ytd, %"].astype(float)
technology_moex = technology_moex.loc[technology_moex["ytd, %"] >= - 30]
technology_moex["Название"] = [f'{el}'.split(' ')[0]  for el in technology_moex["Название"]]
technology_moex["Название"] = [f'{el}'.split('-')[0]  for el in technology_moex["Название"]]
technology_moex["Название"] = [f'{el}'.replace('ао', '').strip()  for el in technology_moex["Название"]]
technology_moex["Название"] = [f'{el}'.replace('.', '').strip()  for el in technology_moex["Название"]]
technology_moex["Название"] = [f'{el}'.replace('i', '').strip()  for el in technology_moex["Название"]]
technology_moex["Тикер"] = [f'{el}.ME' for el in technology_moex["Тикер"]]
technology_moex = technology_moex.loc[technology_moex['Тикер'] != 'RLMNP.ME']

technology_spbex = pd.read_html('https://smart-lab.ru/q/spbex/order_by_article_count_comment/desc/?sector_id%5B%5D=108&country_id=1')[0]
technology_spbex.drop(technology_spbex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14]], axis = 1, inplace = True)
technology_spbex["Активность"] = [f'{el}'.replace(' ', '').strip()  for el in technology_spbex["Активность"]]
technology_spbex["Активность"] = technology_spbex["Активность"].astype(int)
technology_spbex = technology_spbex.loc[technology_spbex["Активность"] >= 30]

technology = pd.concat([technology_moex, technology_spbex])
technology.drop(technology.columns[[0]], axis = 1, inplace = True)
technology_list_company_name = technology["Название"].tolist()
technology_list_tic = technology["Тикер"].tolist()
technology_dict = dict(zip(technology_list_company_name, technology_list_tic))

conn = redis.Redis('localhost')

print(json.dumps(technology_dict))
conn.hmset("technology_dict", technology_dict)
