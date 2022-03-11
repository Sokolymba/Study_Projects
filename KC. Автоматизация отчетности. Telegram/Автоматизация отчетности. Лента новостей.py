# импортируем библиотеки
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import seaborn as sns
import telegram
import io
import os
import locale
import datetime as dt
from datetime import datetime, timedelta
from read_db.CH import Getch

locale.setlocale(locale.LC_ALL, '')

# напишем функцию для сборки и отправки отчета в telegram
def news_report (chat = None):
    bot = telegram.Bot(token = '2146781779:AAEp2ClBZ1g39sUCOyDS5hQx7asLGBgtsbY')
    chat_id = chat or 221427850

# словарь для графика
plot_dict = {(0, 0) : {'y' : 'dau', 'title' : 'Уникальные пользователи за 7 дней', 'color' : 'red'},
            (0, 1) : {'y' : 'views', 'title' : 'Количество просмотров за 7 дней', 'color' : 'green'},
            (1, 0) : {'y' : 'likes', 'title' : 'Количество лайков за 7 дней', 'color' : 'pink'},
            (1, 1) : {'y' : 'CTR', 'title' : 'CTR за 7 дней', 'color' : 'orange'}
                }

# функция для отрисовки графика
def get_plot(data):
    fig, axes = plt.subplots(2,2, figsize=(16,10))
    fig.suptitle('Статистика по ленте за предыдущие 7 дней')
    
    for i in range(2):
        for j in range(2):
            ax = sns.lineplot(ax=axes[i, j], data=data, x='date', y=plot_dict[(i, j)]['y'], color=plot_dict[(i, j)]['color'])
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{}'.format(x/1000) + 'K'))
            axes[i, j].set_title(plot_dict[(i, j)]['title'])
            axes[i, j].grid(True)
            axes[i, j].set(xlabel=None)
            axes[i, j].set(ylabel=None)
            for ind, label in enumerate(axes[i, j].get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
    
    # сохраняем график в png
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'news_stat.png'
    plot_object.seek(0)
    plt.close
    return plot_object
    
    
    # датасет с основными метриками за вчерашний день
    data = Getch('''select toDate(time) as date, \
    uniqExact(user_id) as dau, \
    countIf(user_id, action = 'view') as views, \
    countIf(user_id, action = 'like') as likes, \
    round(likes / views * 100, 2) as CTR, \
    views + likes as events, \
    uniqExact(post_id) as posts, \
    round(likes / dau, 1) as lpu \
    from simulator.feed_actions \
    where toDate(time) = yesterday() \
    group by date \
    order by date''').df
    
    # текстовое сообщение для отправки
    message = ("Отчет по новостной ленте за {}: \n\nEvents: {:,d} \
    \nDAU: {:,d} \
    \nViews: {:,d} \
    \nLikes: {:,d} \
    \nCTR, %: {} \
    \nPosts: {:,d} \
    \nLikes per user: {} \
    \n\nГрафики с основными метриками за 7 дней: " \
               .format(datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y'), \
                       data['events'].iloc[0], \
                       data['dau'].iloc[0], \
                       data['views'].iloc[0], \
                       data['likes'].iloc[0], \
                       data['CTR'].iloc[0], \
                       data['posts'].iloc[0], \
                       data['lpu'].iloc[0]                       
                      ))
    
    # датасет с основными метриками за 7 дней
    data_week = Getch('''select toDate(time) as date, \
    uniqExact(user_id) as dau, \
    countIf(user_id, action = 'view') as views, \
    countIf(user_id, action = 'like') as likes, \
    round(likes / views * 100, 2) as CTR, \
    views + likes as events, \
    uniqExact(post_id) as posts, \
    round(likes / dau, 1) as lpu \
    from simulator.feed_actions \
    where toDate(time) > today() - 8 and toDate(time) != today() \
    group by date \
    order by date''').df
    
    # установим параметры для отправки
    plot_object=get_plot(data_week)
    bot.sendMessage(chat_id=chat_id, text=message)
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

# применяем фукнцию    
try:
    news_report()
except Exception as e:
    print(e)