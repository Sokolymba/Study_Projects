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


# установим параметры для графиков
locale.setlocale(locale.LC_ALL, '')
sns.set(rc={'figure.figsize':(12, 7)})
sns.color_palette("tab10")


# напишем фунецию для отправки отчета в Telegram
def test_report (Chat = None):
    bot = telegram.Bot(token = '2146781779:AAEp2ClBZ1g39sUCOyDS5hQx7asLGBgtsbY')
    chat_id = ''
    
    # импортируем данные из Clickhouse
    data = Getch('select * from simulator.feed_actions where toDate(time) = yesterday()').df
    
    # посчитаем метрики
    dau = data['user_id'].nunique()
    views = data.query('action == "view"')['user_id'].count()
    likes = data.query('action == "like"')['user_id'].count()
    ctr = (likes / views * 100).round()
    
    
    # сообщение для отправки в Telegram
    message = ("Отчет по новостной ленте за {}: \n\nDAU: {:,d} \nViews: {:,d} \nLikes: {:,d} \nCTR, %: {} \n\nГрафики ниже:".format(datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y'), dau, views, likes, ctr))
    bot.sendMessage(chat_id=chat_id, text=message)
    
    # посчитаем значения за 7 предыдущих дней
    
    # dau за 7 дней
    dau_7 = Getch("select toDate(time) as t, uniq(user_id) as dau from simulator.feed_actions where t > today() - 8 and t != today() group by t order by t").df
    
    # график
    plt.title('DAU за 7 дней')
    ax = sns.lineplot(data=dau_7, x='t', y='dau', color='red')
    plt.xlabel('Дата')
    plt.ylabel('DAU')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{}'.format(round(x/1000),0) + 'K'))
    plt.tight_layout()
    
    # отправим график в Telegram
    plot_object_dau = io.BytesIO()
    plt.savefig(plot_object_dau)
    plot_object_dau.name = 'dau_for_7_days.png'
    plot_object_dau.seek(0)

    plt.close()
    
    bot.sendPhoto(chat_id = chat_id, photo = plot_object_dau)
    
    
    # views за 7 дней
    views_7 = Getch("select toDate(time) as t, countIf(user_id, action = 'view') as count_views from simulator.feed_actions where t > today() - 8 and t != today() group by t order by t").df
    
    # график
    plt.title('Просмотры за 7 дней')
    ax = sns.lineplot(data=views_7, x='t', y='count_views', color = 'green')
    plt.xlabel('Дата')
    plt.ylabel('Количество просмотров')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{}'.format(round(x/1000),0) + 'K'))
    plt.tight_layout()

    # отправим график в Telegram
    plot_object_views = io.BytesIO()
    plt.savefig(plot_object_views)
    plot_object_views.name = 'views_for_7_days.png'
    plot_object_views.seek(0)

    plt.close()

    bot.sendPhoto(chat_id = chat_id, photo = plot_object_views)
    
    
    # likes за 7 дней
    likes_7 = Getch("select toDate(time) as t, countIf(user_id, action = 'like') as count_likes from simulator.feed_actions where t > today() - 8 and t != today() group by t order by t").df
    
    # график
    plt.title('Лайки за 7 дней')
    ax = sns.lineplot(data=likes_7, x='t', y='count_likes', color='pink')
    plt.xlabel('Дата')
    plt.ylabel('Количество лайков')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{}'.format(round(x/1000),0) + 'K'))
    plt.tight_layout()


    # отправим график в Telegram
    plot_object_likes = io.BytesIO()
    plt.savefig(plot_object_likes)
    plot_object_likes.name = 'likes_for_7_days.png'
    plot_object_likes.seek(0)

    plt.close()

    bot.sendPhoto(chat_id = chat_id, photo = plot_object_likes)
    
    
    # ctr за 7 дней
    ctr_7 = views_7.merge(likes_7, how='inner', on='t')
    ctr_7['ctr'] = (ctr_7['count_likes'] / ctr_7['count_views'] * 100).round(1)
    
    # график
    plt.title('CTR за 7 дней')
    sns.lineplot(data=ctr_7, x='t', y='ctr', color='orange')
    plt.xlabel('Дата')
    plt.ylabel('CTR, %')
    plt.tight_layout()


    # отправим график в Telegram
    plot_object_ctr = io.BytesIO()
    plt.savefig(plot_object_ctr)
    plot_object_ctr.name = 'ctr_for_7_days.png'
    plot_object_ctr.seek(0)
    
    plt.close()

    bot.sendPhoto(chat_id = chat_id, photo = plot_object_ctr)
    
# запустим функцию для отправки отчета    
try:
    test_report()
except Exception as e:
    print(e)          