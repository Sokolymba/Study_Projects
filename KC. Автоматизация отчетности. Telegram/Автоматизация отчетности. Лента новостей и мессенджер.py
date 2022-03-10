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
from matplotlib.dates import DateFormatter
from read_db.CH import Getch


# установим параметры
locale.setlocale(locale.LC_ALL, '')
sns.set(rc={'figure.figsize':(12, 7)})

# напишем функцию для отправки отчета в Telegram
def test_report (Chat = None):
    bot = telegram.Bot(token = '2146781779:AAEp2ClBZ1g39sUCOyDS5hQx7asLGBgtsbY')
    chat_id = ''
    
    # импортируем данные из Clickhouse
    
    # датасет с пользователями новостной ленты и мессенджера
    data = Getch("select user_id, city \
    from simulator.feed_actions \
    inner join simulator.message_actions on feed_actions.user_id = message_actions.user_id \
    where toDate(time) = today() - 1").df
    dau = data['user_id'].nunique()
    
    # dau только для пользователей новостной ленты 
    data_news = Getch("select user_id from \
    (select distinct(user_id) from simulator.feed_actions where toDate(time) = today() - 1) as t1 \
    left outer join \
    (select distinct(user_id) from simulator.message_actions where toDate(time) = today() - 1) as t2 \
    on t1.user_id = t2.user_id").df
    d_metric = data_news['user_id'].nunique()
    
    # расчет количества уникальных чатов для пользователей мессенджера
    data_messages = Getch("select user_id \
    from simulator.message_actions where toDate(time) = today() - 1").df
    m_metric = data_messages['user_id'].nunique()
    
    # расчет количества уникальных постов
    posts = Getch("select post_id \
    from simulator.feed_actions where toDate(time) = today() - 1").df
    p_metric = posts['post_id'].nunique()
    
    
    # шаблон для отправки сообщения
    message = ("Отчет по ленте и сообщениями за {} \n\nDAU - новостная лента и сообщения: {:,d} \nDAU - новостная лента: {:,d}\nУникальные посты: {:,d} \nУникальные чаты: {:,d} \n\nГрафики по основным метрикам ниже: ".format(datetime.strftime(datetime.now() - timedelta(1), '%d-%m-%Y'), dau, d_metric, p_metric, m_metric))
    bot.sendMessage(chat_id=chat_id, text=message)
    
    # топ-10 городов по количеству пользователей
    top = data.groupby('city').agg({'user_id' : 'count'}).sort_values(by='user_id', ascending=False).reset_index().head(10)
    
    # график
    plt.title('Топ-10 городов по количеству пользователей')
    plt.xticks(rotation=30)
    ax = sns.barplot(x="city", y="user_id", data=top)
    plt.xlabel('Город')
    plt.ylabel('Количество пользователей')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{}'.format(round(x/1000),0) + 'K'))
    plt.tight_layout()
    
    # отправим график в Telegram
    plot_object_top = io.BytesIO()
    plt.savefig(plot_object_top)
    plot_object_top.name = 'top_cities.png'
    plot_object_top.seek(0)

    plt.close()

    bot.sendPhoto(chat_id = chat_id, photo = plot_object_top)
    
    # метрики новостной ленты за 7 дней
    feed_7 = Getch("select toDate(time) as t, uniq(message_actions.user_id) as dau, uniq(post_id) as uniq_posts, gender from simulator.feed_actions inner join simulator.message_actions on feed_actions.user_id = message_actions.user_id where t > today() - 8 and t != today() group by t, gender order by t").df

    # нарисуем график dau за 7 дней
    plt.title('DAU за 7 дней')
    ax = sns.lineplot(data=feed_7, x='t', y='dau', color='red')
    plt.xlabel('Дата')
    plt.ylabel('DAU')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{}'.format(x/1000) + 'K'))
    plt.tight_layout()

    # отправим график в Telegram
    plot_object_dau = io.BytesIO()
    plt.savefig(plot_object_dau)
    plot_object_dau.name = 'dau_for_7_days.png'
    plot_object_dau.seek(0)

    plt.close()
    
    bot.sendPhoto(chat_id = chat_id, photo = plot_object_dau)
    
    
    # нарисуем график количества новых постов за 7 дней
    plt.title('Новые посты за 7 дней')
    sns.lineplot(data=feed_7, x='t', y='uniq_posts', color='pink')
    plt.xlabel('Дата')
    plt.ylabel('Количество новых постов')
    plt.tight_layout()

    # отправим график в Telegram
    plot_object_new_posts = io.BytesIO()
    plt.savefig(plot_object_new_posts)
    plot_object_new_posts.name = 'new_posts_for_7_days.png'
    plot_object_new_posts.seek(0)

    plt.close()
    
    bot.sendPhoto(chat_id = chat_id, photo = plot_object_new_posts)
    
    # график количества пользователей всего приложения за 7 дней с разбивкой по гендеру
    plt.title('Пользователи ленты и сообщений по гендеру за 7 дней')
    ax = sns.lineplot(data=feed_7, x='t', y='dau', hue = 'gender', palette = 'husl')
    plt.xlabel('Дата')
    plt.ylabel('Количество пользователей')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{}'.format(x/1000) + 'K'))
    plt.tight_layout()

    # отправим график в telegram
    plot_object_data_gender = io.BytesIO()
    plt.savefig(plot_object_data_gender)
    plot_object_data_gender.name = 'dau_gender_for_7_days.png'
    plot_object_data_gender.seek(0)

    plt.close()
    
    bot.sendPhoto(chat_id = chat_id, photo = plot_object_data_gender)
    
    # метрики мессенджера за 7 дней
    msgs_7 = Getch("select toDate(time) as t, uniq(reciever_id) as uniq_chats, count(user_id) as os_count, os from simulator.message_actions where t > today() - 8 and t != today() group by t, os order by t").df
    
    # график количества уникальных чатов за 7 дней
    plt.title('Количество уникальных чатов за 7 дней')
    ax = sns.lineplot(data=msgs_7, x='t', y='uniq_chats', color='violet')
    plt.xlabel('Дата')
    plt.ylabel('Количество чатов')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{}'.format(x/1000) + 'K'))
    plt.tight_layout()

    # отправим график в Telegram
    plot_object_chats = io.BytesIO()
    plt.savefig(plot_object_chats)
    plot_object_chats.name = 'uniq_chats_for_7_days.png'
    plot_object_chats.seek(0)

    plt.close()
    
    bot.sendPhoto(chat_id = chat_id, photo = plot_object_chats)
    
    # график количества отправленных сообщения за 7 дней с разбивкой по операционной системе
    plt.title('Отправленные сообщения с разных OS за 7 дней')
    ax = sns.lineplot(data=msgs_7, x='t', y='os_count', hue = 'os', palette = 'husl')
    plt.xlabel('Дата')
    plt.ylabel('Количество сообщений')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{}'.format(x/1000) + 'K'))
    plt.tight_layout()

    # отправим график в Telegram
    plot_object_data_os = io.BytesIO()
    plt.savefig(plot_object_data_os)
    plot_object_data_os.name = 'count_os_for_7_days.png'
    plot_object_data_os.seek(0)

    plt.close()
    
    bot.sendPhoto(chat_id = chat_id, photo = plot_object_data_os)
    

try:
    test_report()
except Exception as e:
    print(e)