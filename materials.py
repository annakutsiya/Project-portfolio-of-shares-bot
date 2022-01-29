#материалы

import pandas as pd
import yfinance as yf
import numpy as np
import redis
import json

materials_moex = pd.read_html('https://smart-lab.ru/q/shares/order_by_last_to_year_price/desc/?sector_id%5B%5D=3&sector_id%5B%5D=21&sector_id%5B%5D=22&sector_id%5B%5D=23&sector_id%5B%5D=18&sector_id%5B%5D=17&sector_id%5B%5D=24')[0]
materials_moex.drop(materials_moex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19]], axis = 1, inplace = True)
materials_moex["ytd, %"] = [f'{el}'.replace('+', '').strip()  for el in materials_moex["ytd, %"]]
materials_moex["ytd, %"] = [f'{el}'.replace('%', '').strip()  for el in materials_moex["ytd, %"]]
materials_moex["ytd, %"] = [f'{el}'.replace('nan', '-100').strip()  for el in materials_moex["ytd, %"]]#УСЛОВНО
materials_moex["ytd, %"] = materials_moex["ytd, %"].astype(float)
materials_moex = materials_moex.loc[materials_moex["ytd, %"] >= 10]
materials_moex["Название"] = [f'{el}'.split(' ')[0]  for el in materials_moex["Название"]]
materials_moex["Название"] = [f'{el}'.split('-')[0]  for el in materials_moex["Название"]]
materials_moex["Название"] = [f'{el}'.replace('ао', '').strip()  for el in materials_moex["Название"]]
materials_moex["Название"] = [f'{el}'.replace('.', '').strip()  for el in materials_moex["Название"]]
materials_moex["Тикер"] = [f'{el}.ME' for el in materials_moex["Тикер"]]


materials_spbex = pd.read_html('https://smart-lab.ru/q/spbex/order_by_article_count_comment/desc/?sector_id%5B%5D=110&country_id=1')[0]
materials_spbex.drop(materials_spbex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14]], axis = 1, inplace = True)
materials_spbex = materials_spbex.loc[materials_spbex["Активность"] >= 20]


materials = pd.concat([materials_moex, materials_spbex])
materials.drop(materials.columns[[0]], axis = 1, inplace = True)
materials_list_company_name = materials["Название"].tolist()
materials_list_tic = materials["Тикер"].tolist()
materials_dict = dict(zip(materials_list_company_name, materials_list_tic))

conn = redis.Redis('localhost')

print(json.dumps(materials_dict))
conn.hmset("materials_dict", materials_dict)
