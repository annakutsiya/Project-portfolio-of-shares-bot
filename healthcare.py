#здравоохранение

import pandas as pd
import yfinance as yf
import numpy as np
import redis
import json

healthcare = pd.read_html('https://smart-lab.ru/q/spbex/order_by_article_count_comment/desc/?sector_id%5B%5D=106&country_id=1')[0]
healthcare.drop(healthcare.columns[[0, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14]], axis = 1, inplace = True)
healthcare = healthcare.loc[healthcare["Активность"] >= 30]

healthcare_list_company_name = healthcare["Название"].tolist()
healthcare_list_tic = healthcare["Тикер"].tolist()
healthcare_dict = dict(zip(healthcare_list_company_name, healthcare_list_tic))

conn = redis.Redis('localhost')

print(json.dumps(healthcare_dict))
conn.hmset("healthcare_dict", healthcare_dict)
