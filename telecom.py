#телеком

import pandas as pd
import yfinance as yf
import numpy as np
import redis
import json

tele_moex = pd.read_html('https://smart-lab.ru/q/shares/order_by_last_to_year_price/desc/?sector_id%5B%5D=6&sector_id%5B%5D=25')[0]
tele_moex.drop(tele_moex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19]], axis = 1, inplace = True)
tele_moex["ytd, %"] = [f'{el}'.replace('+', '').strip()  for el in tele_moex["ytd, %"]]
tele_moex["ytd, %"] = [f'{el}'.replace('%', '').strip()  for el in tele_moex["ytd, %"]]
tele_moex["ytd, %"] = [f'{el}'.replace('nan', '-100').strip()  for el in tele_moex["ytd, %"]]#УСЛОВНО
tele_moex["ytd, %"] = tele_moex["ytd, %"].astype(float)
tele_moex = tele_moex.loc[tele_moex["ytd, %"] >= -25]
tele_moex["Название"] = [f'{el}'.split(' ')[0]  for el in tele_moex["Название"]]
tele_moex["Название"] = [f'{el}'.split('-')[0]  for el in tele_moex["Название"]]
tele_moex["Название"] = [f'{el}'.replace('ао', '').strip()  for el in tele_moex["Название"]]
tele_moex["Название"] = [f'{el}'.replace('.', '').strip()  for el in tele_moex["Название"]]
tele_moex["Тикер"] = [f'{el}.ME' for el in tele_moex["Тикер"]]


tele_spbex = pd.read_html('https://smart-lab.ru/q/spbex/order_by_article_count_comment/desc/?sector_id%5B%5D=109&country_id=1')[0]
tele_spbex.drop(tele_spbex.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14]], axis = 1, inplace = True)
tele_spbex = tele_spbex.loc[tele_spbex["Активность"] >= 30]


telecom = pd.concat([tele_moex, tele_spbex])
telecom.drop(telecom.columns[[0]], axis = 1, inplace = True)
telecom_list_company_name = telecom["Название"].tolist()
telecom_list_tic = telecom["Тикер"].tolist()
telecom_dict = dict(zip(telecom_list_company_name, telecom_list_tic))

conn = redis.Redis('localhost')

print(json.dumps(telecom_dict))
conn.hmset("telecom_dict", telecom_dict)
