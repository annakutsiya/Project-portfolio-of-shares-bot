#энергетика

import pandas as pd
import yfinance as yf
import numpy as np
import redis
import json

energy_moex = pd.read_html('https://smart-lab.ru/q/shares/order_by_last_to_year_price/desc/?sector_id%5B%5D=1')[0]
energy_moex.drop(energy_moex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19]], axis = 1, inplace = True)
energy_moex["ytd, %"] = [f'{el}'.replace('+', '').strip()  for el in energy_moex["ytd, %"]]
energy_moex["ytd, %"] = [f'{el}'.replace('%', '').strip()  for el in energy_moex["ytd, %"]]
energy_moex["ytd, %"] = [f'{el}'.replace('nan', '-100').strip()  for el in energy_moex["ytd, %"]]#УСЛОВНО
energy_moex["ytd, %"] = energy_moex["ytd, %"].astype(float)
energy_moex = energy_moex.loc[energy_moex["ytd, %"] >= 10]
energy_moex["Название"] = [f'{el}'.split(' ')[0]  for el in energy_moex["Название"]]
energy_moex["Название"] = [f'{el}'.split('-')[0]  for el in energy_moex["Название"]]
energy_moex["Название"] = [f'{el}'.replace('ао', '').strip()  for el in energy_moex["Название"]]
energy_moex["Название"] = [f'{el}'.replace('.', '').strip()  for el in energy_moex["Название"]]
energy_moex["Тикер"] = [f'{el}.ME' for el in energy_moex["Тикер"]]
energy_moex = energy_moex.loc[energy_moex['Тикер'] != 'TATNP.ME']
energy_moex = energy_moex.loc[energy_moex['Тикер'] != 'VJGZP.ME']
energy_moex = energy_moex.loc[energy_moex['Тикер'] != 'MFGSP.ME']
energy_moex = energy_moex.loc[energy_moex['Тикер'] != 'SNGSP.ME']
energy_moex = energy_moex.loc[energy_moex['Тикер'] != 'KRKNP.ME']
energy_moex = energy_moex.loc[energy_moex['Тикер'] != 'BANEP.ME']

energy_spbex = pd.read_html('https://smart-lab.ru/q/spbex/order_by_article_count_comment/desc/?sector_id%5B%5D=105&country_id=1')[0]
energy_spbex.drop(energy_spbex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14]], axis = 1, inplace = True)
energy_spbex = energy_spbex.loc[energy_spbex["Активность"] >= 20]

energy = pd.concat([energy_moex, energy_spbex])
energy.drop(energy.columns[[0]], axis = 1, inplace = True)
energy_list_company_name = energy["Название"].tolist()
energy_list_tic = energy["Тикер"].tolist()
energy_dict = dict(zip(energy_list_company_name, energy_list_tic))

conn = redis.Redis('localhost')

print(json.dumps(energy_dict))
conn.hmset("energy_dict", energy_dict)
#print(json.dumps(energy_dict))
#for key, value in energy_dict.items():
 #   conn.hset("energy_dict", key, value)