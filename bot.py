import logging
from queue import PriorityQueue
from typing import MutableMapping
import pandas as pd
import yfinance as yf
import numpy as np
import redis
import json 
import pandas_datareader as web
import matplotlib.pyplot as plt
import telebot
import config
from pypfopt.expected_returns import mean_historical_return
from pypfopt import risk_models 
from pypfopt import expected_returns
from pypfopt.cla import CLA
import pypfopt.plotting as pplt
from matplotlib.ticker import FuncFormatter
from pypfopt.risk_models import CovarianceShrinkage
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices

from keyboa import keyboa_maker
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, ConversationHandler
from telegram import ReplyKeyboardMarkup, KeyboardButton

import finance
import portfolio

from datetime import datetime
import settings

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO,
                    filename='bot.log')


collect_companies = False
companies_list = []
tic_list= []

PROXY = {
    'proxy_url': 'socks5://t1.learn.python.ru:1080',
    'urllib3_proxy_kwargs': {
        'username': 'learn',
        'password': 'python'
    }
}

conn = redis.Redis('localhost')
finance_dict = conn.hgetall("finance_dict")
new_finance_dict = {}
finance_comp_name = []
for key, value in finance_dict.items():
    new_finance_dict[key.decode('utf-8')] = value.decode('utf-8')
for key, value in finance_dict.items():
    finance_comp_name.append(key.decode('utf-8'))

conn = redis.Redis('localhost')
discretionary_dict = conn.hgetall("discretionary_dict") 
new_discretionary_dict = {}
discretionary_comp_name = []
for key, value in discretionary_dict.items():
    new_discretionary_dict[key.decode('utf-8')] = value.decode('utf-8')
for key, value in discretionary_dict.items():
    discretionary_comp_name.append(key.decode('utf-8'))

conn = redis.Redis('localhost')
energy_dict = conn.hgetall("energy_dict") 
new_energy_dict = {}
energy_comp_name = []
for key, value in energy_dict.items():
    new_energy_dict[key.decode('utf-8')] = value.decode('utf-8')
for key, value in energy_dict.items():
    energy_comp_name.append(key.decode('utf-8'))

conn = redis.Redis('localhost')
healthcare_dict = conn.hgetall("healthcare_dict") 
new_healthcare_dict = {}
healthcare_comp_name = []
for key, value in healthcare_dict.items():
    new_healthcare_dict[key.decode('utf-8')] = value.decode('utf-8')
for key, value in healthcare_dict.items():
    healthcare_comp_name.append(key.decode('utf-8'))

conn = redis.Redis('localhost')
industrials_dict = conn.hgetall("industrials_dict") 
new_industrials_dict = {}
industrials_comp_name = []
for key, value in industrials_dict.items():
    new_industrials_dict[key.decode('utf-8')] = value.decode('utf-8')
for key, value in industrials_dict.items():
    industrials_comp_name.append(key.decode('utf-8'))

conn = redis.Redis('localhost')
materials_dict = conn.hgetall("materials_dict") 
new_materials_dict = {}
materials_comp_name = []
for key, value in materials_dict.items():
    new_materials_dict[key.decode('utf-8')] = value.decode('utf-8')
for key, value in materials_dict.items():
    materials_comp_name.append(key.decode('utf-8'))

conn = redis.Redis('localhost')
staples_dict = conn.hgetall("staples_dict") 
new_staples_dict = {}
staples_comp_name = []
for key, value in staples_dict.items():
    new_staples_dict[key.decode('utf-8')] = value.decode('utf-8')
for key, value in staples_dict.items():
    staples_comp_name.append(key.decode('utf-8'))

conn = redis.Redis('localhost')
technology_dict = conn.hgetall("technology_dict") 
new_technology_dict = {}
technology_comp_name = []
for key, value in technology_dict.items():
    new_technology_dict[key.decode('utf-8')] = value.decode('utf-8')
for key, value in technology_dict.items():
    technology_comp_name.append(key.decode('utf-8'))

conn = redis.Redis('localhost')
telecom_dict = conn.hgetall("telecom_dict") 
new_telecom_dict = {}
telecom_comp_name = []
for key, value in telecom_dict.items():
    new_telecom_dict[key.decode('utf-8')] = value.decode('utf-8')
for key, value in telecom_dict.items():
    telecom_comp_name.append(key.decode('utf-8'))

global_dict = {**new_discretionary_dict, **new_energy_dict, **new_finance_dict, 
**new_healthcare_dict, **new_industrials_dict, **new_materials_dict, **new_staples_dict,
**new_technology_dict, **new_telecom_dict}
global_keys = global_dict.keys()

def greet_user(update, context):
    print("Вызван /start")
    update.message.reply_text(
        f"Привет! Вызови команду /help.")

def help_command(update, context):   
    update.message.reply_text(
        f"1. Выбери сектор, акции которого тебе интересны.")
    update.message.reply_text(
        f"2.Выбери компании, которые тебе интересны, из предложенных списков(вводи название и отправляй).")
    update.message.reply_text(
        f"3. Используй команду /tic, чтобы получить список тикеров.")
    update.message.reply_text(
        f"3. Используй команду /portfolio, чтобы составить портфель из выбранных компаний.")
    update.message.reply_text(
        f"3. Используй команду /chart, чтобы вывести графики портфеля.")    
    update.message.reply_text(
        f"4. Если клавиатура снова понадобится, то вызови команду /keyboard.", 
        reply_markup= main_keyboard())

def get_keyboard(update, context):
    update.message.reply_text(
        f"Возвращаю клавиатуру.", 
        reply_markup= main_keyboard()
        )
      
def main_keyboard():
    return ReplyKeyboardMarkup([['Финансы'], ['Потреб'], ['Энергетика'], ['Телеком'],
    ['Материалы'], ['Технологии'], ['Здравоохранение'], ['Промышленность'], ['Ритэйл']])


def finance_handler(update, context):
    text = update.message.text
    global collect_companies 
    collect_companies = True
    my_keyboard = ReplyKeyboardMarkup([['Финансы']], True)
    update.message.reply_text(\
    f"{'В секторе финансы я знаю такие компании:'} {', '.join(finance_comp_name)}",\
        reply_markup=main_keyboard())
    return collect_companies

def discretionary_handler(update, context):
    text = update.message.text
    global collect_companies
    collect_companies = True
    my_keyboard = ReplyKeyboardMarkup([['Потреб']], True)
    update.message.reply_text(\
    f"{'В секторе вторичной необходимости я знаю такие компании:'} {', '.join(discretionary_comp_name)}",\
        reply_markup=main_keyboard())
    return collect_companies

def energy_handler(update, context):
    text = update.message.text
    global collect_companies
    collect_companies = True
    my_keyboard = ReplyKeyboardMarkup([['Энергетика']], True)
    update.message.reply_text(\
    f"{'В секторе энергетика я знаю такие компании:'} {', '.join(energy_comp_name)}",\
        reply_markup=main_keyboard())
    return collect_companies

def healthcare_handler(update, context):
    text = update.message.text
    global collect_companies
    collect_companies = True
    my_keyboard = ReplyKeyboardMarkup([['Здравоохранение']], True)
    update.message.reply_text(\
    f"{'В секторе здравоохранение я знаю такие компании:'} {', '.join(healthcare_comp_name)}",\
        reply_markup=main_keyboard())
    return collect_companies

def industrials_handler(update, context):
    text = update.message.text
    global collect_companies
    collect_companies = True
    my_keyboard = ReplyKeyboardMarkup([['Промышленность']], True)
    update.message.reply_text(\
    f"{'В секторе промышленность я знаю такие компании:'} {', '.join(industrials_comp_name)}",\
        reply_markup=main_keyboard())
    return collect_companies

def materials_handler(update, context):
    text = update.message.text
    global collect_companies
    collect_companies = True
    my_keyboard = ReplyKeyboardMarkup([['Материалы']], True)
    update.message.reply_text(\
    f"{'В секторе материалы я знаю такие компании:'} {', '.join(materials_comp_name)}",\
        reply_markup=main_keyboard())
    return collect_companies

def staples_handler(update, context):
    text = update.message.text
    global collect_companies
    collect_companies = True
    my_keyboard = ReplyKeyboardMarkup([['Ритэйл']], True)
    update.message.reply_text(\
    f"{'В секторе розничной торговли я знаю такие компании:'} {', '.join(staples_comp_name)}",\
        reply_markup=main_keyboard())
    return collect_companies

def technology_handler(update, context):
    text = update.message.text
    global collect_companies
    collect_companies = True
    my_keyboard = ReplyKeyboardMarkup([['Технологии']], True)
    update.message.reply_text(\
    f"{'В секторе технологии я знаю такие компании:'} {', '.join(technology_comp_name)}",\
        reply_markup=main_keyboard())
    return collect_companies

def tele_handler(update, context):
    text = update.message.text
    global collect_companies
    collect_companies = True
    my_keyboard = ReplyKeyboardMarkup([['Телеком']], True)
    update.message.reply_text(\
    f"{'В секторе телеком я знаю такие компании:'} {', '.join(telecom_comp_name)}",\
        reply_markup=main_keyboard())
    return collect_companies

def collecting_user_data(update, context):
    global collect_companies
    global companies_list
    print(collect_companies)
    user_input = update.message.text
    if collect_companies:
        if user_input not in companies_list:
            companies_list.append(user_input)
        else:
            update.message.reply_text(f"Такое название уже есть :(", reply_markup=main_keyboard())
        for el in companies_list:
            if el not in global_keys:
                update.message.reply_text('Ошибка в названии компании, попробуй еще раз :(',
                reply_markup=main_keyboard())
                companies_list.remove(el)
        update.message.reply_text(f"{'Ваше сообщение:'} {', '.join(companies_list)}", \
        reply_markup=main_keyboard())
    else:
        print('button off')
    return None

      
def tic(update, context):   
    global tic_list 
    for name in companies_list:
        name_to_append = global_dict.get(name)
        if name_to_append not in tic_list:
            tic_list.append(name_to_append)
    print("TIC LIST", tic_list)
    update.message.reply_text(f"{'Ваши тикеры:'} {', '.join(tic_list)}")
    return tic_list

def portfolio_construct(update, context):
    global data 
    global mu 
    global S 
    global ex 
    global weights
    global cleaned_weights
    data = pd.DataFrame(columns=tic_list)
    for ticker in tic_list:
        data[ticker] = yf.download(ticker, start = today - timedelta(days=365), end=today) ['Adj Close']
    mu = mean_historical_return(data)
    S = risk_models.sample_cov(data)
    ef = EfficientFrontier(mu, S, weight_bounds = (0,1))
    weights = ef.max_sharpe()
    cleaned_weights = ef.clean_weights()
    print(cleaned_weights)
    update.message.reply_text(f'{cleaned_weights}')
    return cleaned_weights
def my_portfolio_chart (update, context):
    global tickers
    global t_weights
    tickers =[]
    t_weights =[]

    for i in cleaned_weights:
  
        if cleaned_weights[i] > 0:
             t_weights.append(cleaned_weights[i])
             tickers.append(i)
    cl_obj = CLA(mu, S)
    ax = pplt.plot_efficient_frontier(cl_obj, showfig = False)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: '{:.0%}'.format(x)))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
    fig1, ax1 = plt.subplots()
    ax1.pie(t_weights, labels=tickers)
    ax1.axis('equal')
    patches, texts, auto = ax1.pie(t_weights, startangle=90, autopct='%1.1f%%' )
    plt.legend(patches, tickers, loc="best")
    portfolio_chart = plt.savefig('portfilio_chart.png', facecolor = 'blue', bbox_inches='tight', dpi=50 )
    context.bot.send_photo(photo=open(portfolio_chart))
    print(f'построение графика')
    return portfolio_chart

        

def main():
    mybot = Updater(settings.API_KEY, use_context=True, request_kwargs=PROXY)
    dp = mybot.dispatcher
    #if collect_companies:
     #   dp.add_handler(MessageHandler(Filters.text, talk_to_me_2))
    dp.add_handler(CommandHandler("start", greet_user))
    dp.add_handler(MessageHandler(Filters.regex('^Финансы'), finance_handler))
    
    dp.add_handler(MessageHandler(Filters.regex('^Потреб'), discretionary_handler))
    dp.add_handler(MessageHandler(Filters.regex('^Энергетика'), energy_handler))
    dp.add_handler(MessageHandler(Filters.regex('^Здравоохранение'), healthcare_handler))
    dp.add_handler(MessageHandler(Filters.regex('^Промышленность'), industrials_handler))
    dp.add_handler(MessageHandler(Filters.regex('^Материалы'), materials_handler))
    dp.add_handler(MessageHandler(Filters.regex('^Ритэйл'), staples_handler))
    dp.add_handler(MessageHandler(Filters.regex('^Технологии'), technology_handler))
    dp.add_handler(MessageHandler(Filters.regex('^Телеком'), tele_handler))
    dp.add_handler(CommandHandler("tic", tic))
    dp.add_handler(CommandHandler("help", help_command))
    dp.add_handler(CommandHandler("keyboard", get_keyboard))
    dp.add_handler(MessageHandler(Filters.text, collecting_user_data))

    dp.add_handler(CommandHandler("portfolio", portfolio_construct))
    # dp.add_handler(MessageHandler(Filters.text, talk_to_me_2))
    #dp.add_handler(CommandHandler("my_budget_portfolio", my_budget_portfolio))
    #dp.add_handler(CommandHandler("my_portfolio_stat", my_portfolio_stat))
    dp.add_handler(CommandHandler("chart", my_portfolio_chart))

    logging.info("Бот стартовал")
    mybot.start_polling()
    mybot.idle()

if __name__ == "__main__":
    main()