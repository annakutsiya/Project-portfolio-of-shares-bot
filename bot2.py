import logging
import pandas as pd
import yfinance as yf
import numpy as np
import redis
import json
import ephem 
import pandas_datareader as web
import matplotlib.pyplot as plt
#import telebot
#import config
from pypfopt.expected_returns import mean_historical_return
from pypfopt import risk_models 
from pypfopt import expected_returns
from pypfopt.cla import CLA
import pypfopt.plotting as pplt
from matplotlib.ticker import FuncFormatter
from pypfopt.risk_models import CovarianceShrinkage
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices
import finance
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, ConversationHandler
from telegram import ReplyKeyboardMarkup, KeyboardButton
from datetime import datetime
import settings
logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO,
                    filename='bot.log')

#TOKEN = API_KEY
#bot = telebot.TeleBot(config.TOKEN)


PROXY = {
    'proxy_url': 'socks5://t1.learn.python.ru:1080',
    'urllib3_proxy_kwargs': {
        'username': 'learn',
        'password': 'python'
    }
}


conn = redis.Redis('localhost')
finance_comp_name = []
for key, value in conn.hgetall("finance_dict").items():
    finance_comp_name.append(key.decode('utf-8'))


def final_button():
    return ReplyKeyboardMarkup([['Все']])


def main_keyboard():
    return ReplyKeyboardMarkup([['Финансы']])

def greet_user(update, context):
    print("Вызван /start")
    update.message.reply_text(
        f"Привет! Выбери сектор.",
        reply_markup=main_keyboard()
    )

   
#def help_command(message):  
 #   keyboard = telebot.types.InlineKeyboardMarkup()  
  #  keyboard.add(  
   #     telebot.types.InlineKeyboardButton(  
    #        'Message the developer', url='telegram.me/artiomtb'  
  #)  
   # )  
    #bot.send_message(  
     #   message.chat.id, 
      #  '1) Выбери сектор, акции которого тебе интересны.\n' + 
      #  '2) Выбери до (кол-во) компаний, интресеных тебе в данном сеторе.\n'+
       # '3) После получения состава портфеля реализуй команду /my_budget_portfolio , чтобы узнать количество акций, которые ты можешь купить относительно твоего бюджета.\n'+
       # '4) Реализуй команду /my_portfolio_stat чтобы узнать краткий свод статистики по своему портфелю.\n'+
       # '5) Реализуй команду /my_portfolio_chart чтобы посмотреть график твоего портфеля и диаграмму распределения акций в нем.'+
       # reply_markup = keyboard
   # )
def talk_to_me(update, context):
    text = update.message.text
    my_keyboard = ReplyKeyboardMarkup([['Финансы']], True)
    update.message.reply_text(f"{'В секторе финансы я знаю такие компании:'} {', '.join(finance_comp_name)}", reply_markup=final_button())

def talk_to_me_2(update, context):
    user_text = update.message.text
    print(user_text)

def portfolio_construct (list, date):
    time_shares = yf.download(list, start = date - timedata(days=365), end=date) ['Adj Close']
    mu = mean_historical_return(time_shares)
    S = CovarianceShrinkage(time_shares).ledoit_wolf()
    ef = EfficientFrontier(mu, S, weight_bounds = (0,1))
    weights = ef.max_sharpe()
    cleaned_weights = ef.clean_weights()
    update.message.reply_text(cleaned_weights)

def my_budget_portfolio (update, context, message):
    bot.send_message(message.chat.id, 'Введите сумму')
    sum = message.text()
    latest_prices = get_latest_prices(time_shares)
    da = DiscreteAllocation(weights, latest_prices, total_portfolio_value = sum)
    allocation, leftover = da.lp_portfolio()
    bot.send_message(message.chat.id, f' Потратив {sum}, вы сможете купить такое количество акций от каждой компании в эффективном портфеле: {allocation}')

def my_portfolio_stat (update, context, message):
    update.message.reply_text(ef.portfolio_performance(verbose=True))

def my_portfolio_chart (update, context, message):
    cl_obj = CLA(mu, S)
    ax = pplt.plot_efficient_frontier(cl_obj, showfig = False)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: '{:.0%}'.format(x)))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
    fig1, ax1 = plt.subplots()
    ax1.pie(t_weights, labels=tickers)
    ax1.axis('equal')
    patches, texts, auto = ax1.pie(t_weights, startangle=90, autopct='%1.1f%%' )
    plt.legend(patches, tickers, loc="best")
    bot.message.reply_text(ax, plt.show())

def test_func(update, context, message):
    update.message.reply_text('athumn!')

def main():
    mybot = Updater(settings.API_KEY, use_context=True, request_kwargs=PROXY)

    dp = mybot.dispatcher
    dp.add_handler(CommandHandler("start", greet_user))
    dp.add_handler(MessageHandler(Filters.regex('^Финансы'), talk_to_me))
    dp.add_handler(MessageHandler(Filters.regex('^Все'), greet_user))
    dp.add_handler(MessageHandler(Filters.text, test_func))
    #dp.add_handler(CommandHandler("help", help_command))

    # dp.add_handler(MessageHandler(Filters.text, portfolio_construct))
    # dp.add_handler(MessageHandler(Filters.text, talk_to_me_2))
    dp.add_handler(CommandHandler("my_budget_portfolio", my_budget_portfolio))
    dp.add_handler(CommandHandler("my_portfolio_stat", my_portfolio_stat))
    dp.add_handler(CommandHandler("my_portfolio_chart", my_portfolio_chart))

    logging.info("Бот стартовал")
    mybot.start_polling()
    mybot.idle()

if __name__ == "__main__":
    main()