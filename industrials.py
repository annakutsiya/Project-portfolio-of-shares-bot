#промышленность

import pandas as pd
import yfinance as yf
import numpy as np
import redis
import json

industrials_moex = pd.read_html('https://smart-lab.ru/q/shares/order_by_last_to_year_price/desc/?sector_id%5B%5D=7&sector_id%5B%5D=9')[0]
industrials_moex.drop(industrials_moex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19]], axis = 1, inplace = True)
industrials_moex["ytd, %"] = [f'{el}'.replace('+', '').strip()  for el in industrials_moex["ytd, %"]]
industrials_moex["ytd, %"] = [f'{el}'.replace('%', '').strip()  for el in industrials_moex["ytd, %"]]
industrials_moex["ytd, %"] = [f'{el}'.replace('nan', '-100').strip()  for el in industrials_moex["ytd, %"]]#УСЛОВНО
industrials_moex["ytd, %"] = industrials_moex["ytd, %"].astype(float)
industrials_moex = industrials_moex.loc[industrials_moex["ytd, %"] >= 10]
industrials_moex["Название"] = [f'{el}'.split(' ')[0]  for el in industrials_moex["Название"]]
industrials_moex["Название"] = [f'{el}'.split('-')[0]  for el in industrials_moex["Название"]]
industrials_moex["Название"] = [f'{el}'.replace('ао', '').strip()  for el in industrials_moex["Название"]]
industrials_moex["Название"] = [f'{el}'.replace('.', '').strip()  for el in industrials_moex["Название"]]
industrials_moex["Тикер"] = [f'{el}.ME' for el in industrials_moex["Тикер"]]
industrials_moex = industrials_moex.loc[industrials_moex['Тикер'] != 'GAZAP.ME']

industrials_spbex = pd.read_html('https://smart-lab.ru/q/spbex/order_by_article_count_comment/desc/?sector_id%5B%5D=107&country_id=1')[0]
industrials_spbex.drop(industrials_spbex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14]], axis = 1, inplace = True)
industrials_spbex["Активность"] = [f'{el}'.replace(' ', '').strip()  for el in industrials_spbex["Активность"]]
industrials_spbex["Активность"] = industrials_spbex["Активность"].astype(int)
industrials_spbex = industrials_spbex.loc[industrials_spbex["Активность"] >= 20]

industrials = pd.concat([industrials_moex, industrials_spbex])
industrials.drop(industrials.columns[[0]], axis = 1, inplace = True)
industrials_list_company_name = industrials["Название"].tolist()
industrials_list_tic = industrials["Тикер"].tolist()
industrials_dict = dict(zip(industrials_list_company_name, industrials_list_tic))

conn = redis.Redis('localhost')

print(json.dumps(industrials_dict))
conn.hmset("industrials_dict", industrials_dict)
