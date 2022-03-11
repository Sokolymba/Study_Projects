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
from read_db.CH import Getch

sns.set()

# напишем функцию для отрисовки графиков
def get_plot(data_feed, data_msgs, data_new_users, data_dau_all):
    data = pd.merge(data_feed, data_msgs, on='date')
    data = pd.merge(data, data_new_users, on='date')
    data = pd.merge(data, data_dau_all, on='date')
    
    # посчитаем количество всех событий
    data['events_app'] = data['events'] + data['msgs']
    
    plot_objects = []
    
    # графики по всему приложению за 7 дней
    fig, axes = plt.subplots(3, figsize=(10, 14))
    fig.suptitle('Статистика по всему приложению за 7 дней')
    
    # словарь для графиков
    app_dict = {0: {'y' : ['events_app'], 'title' : 'Количество всех событий', 'color' : 'red'},
               1: {'y' : ['users', 'users_ios', 'users_android'], 'title' : 'Уникальные пользователи', 'color' : 'green'},
               2: {'y' : ['new_users', 'new_users_ads', 'new_users_organic'], 'title' : 'Новые пользователи', 'color' : 'yellow'}
               }
    
    # цикл для отрисовки графиков
    for i in range(3):
        for y in app_dict[i]['y']['color']:
            ax = sns.lineplot(ax=axes[i], data=data, x='date', y=y, color='color')
            axes[i].set_title(app_dict[(i)]['title'])
            axes[i].set(xlabel=None)
            axes[i].set(ylabel=None)
            axes[i].legend(app_dict[i]['y'])
            for ind, label in enumerate(axes[i].get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
                
    # сохраняем график в png    
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'app_stat.png'
    plot_object.seek(0)
    plt.close
    plot_objects.append(plot_object)
    
    
    # графики по новостной ленте за 7 дней
    fig, axes = plt.subplots(2, 2, figsize=(14, 14))
    fig.suptitle('Статистика по ленте за предыдущие 7 дней')
    
    # словарь для графиков
    feed_dict = {(0, 0) : {'y' : 'dau_feed', 'title' : 'Уникальные пользователи', 'color' : 'red'},
            (0, 1) : {'y' : 'views', 'title' : 'Количество просмотров', 'color' : 'green'},
            (1, 0) : {'y' : 'likes', 'title' : 'Количество лайков', 'color' : 'pink'},
            (1, 1) : {'y' : 'CTR', 'title' : 'CTR', 'color' : 'orange'}
                }
    
    # цикл для отрисовки графиков
    for i in range(2):
        for j in range(2):
            ax = sns.lineplot(ax=axes[i, j], data=data, x='date', y=feed_dict[(i, j)]['y'], color=feed_dict[(i, j)]['color'])
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{}'.format(x/1000) + 'K'))
            axes[i, j].set_title(feed_dict[(i, j)]['title'])
            axes[i, j].grid(True)
            axes[i, j].set(xlabel=None)
            axes[i, j].set(ylabel=None)
            for ind, label in enumerate(axes[i, j].get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
    
    # сохраняем графики в png
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'feed_stat.png'
    plot_object.seek(0)
    plt.close
    plot_objects.append(plot_object)
    
    
    # графики по мессенджеру за 7 дней
    fig, axes = plt.subplots(3, figsize=(14, 14))
    fig.suptitle('Статистика по мессенджеру за предыдущие 7 дней')
    
    # словарь для графиков
    msgs_dict = {0 : {'y' : 'dau_msgs', 'title' : 'Уникальные пользователи', 'color' : 'red'},
                 1: {'y' : 'msgs', 'title' : 'Сообщения', 'color' : 'green'},
                 2: {'y' : 'mpu', 'title' : 'Количество лайков на одного пользователя', 'color' : 'pink'}
                }
    
    # цикл для отрисовки графиков
    for i in range(3):
            ax = sns.lineplot(ax=axes[i], data=data, x='date', y=msgs_dict[i]['y'], color=msgs_dict[i]['color'])
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{}'.format(x/1000) + 'K'))
            axes[i].set_title(msgs_dict[(i)]['title'])
            axes[i].grid(True)
            axes[i].set(xlabel=None)
            axes[i].set(ylabel=None)
            for ind, label in enumerate(axes[i].get_xticklabels()):
                if ind % 3 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
    
    # сохраняем графики в png
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'msgs_stat.png'
    plot_object.seek(0)
    plt.close
    plot_objects.append(plot_object)

    
    return plot_objects

# напишем функцию для сборки и отправки отчета
def app_report(chat=None):
    chat_id = chat or 221427850
    bot = telegram.Bot(token = '2146781779:AAEp2ClBZ1g39sUCOyDS5hQx7asLGBgtsbY')
    
    # датасет по новостной ленте за вчера и неделю назад
    data_feed = Getch('''
    select toDate(time) as date,
    uniqExact(user_id) as dau_feed,
    countIf(user_id, action = 'view') as views,
    countIf(user_id, action = 'like') as likes,
    round(likes / views * 100, 2) as CTR,
    views + likes as events,
    uniqExact(post_id) as posts,
    round(likes / dau_feed, 1) as lpu
    from simulator.feed_actions
    where toDate(time) between today() - 8 and today() - 1
    group by date
    order by date''').df
    
    # датасет по мессенджеру за вчера и 7 дней назад
    data_msgs = Getch('''
    select toDate(time) as date,
    uniqExact(user_id) as dau_msgs,
    count(user_id) as msgs,
    msgs / dau_msgs as mpu
    from simulator.message_actions
    where toDate(time) between today() - 8 and today() - 1
    group by date
    order by date''').df
    
    # датасет всему приложению за вчера и 7 дней назад
    data_dau_all = Getch('''
    select date,
    uniqExact(user_id) as users,
    uniqExactIf(user_id, os='iOS') as users_ios,
    uniqExactIf(user_id, os='Android') as users_android
    from (
    select distinct
    toDate(time) as date,
    user_id,
    os
    from simulator.feed_actions
    where toDate(time) between today() - 8 and today() - 1
    union all
    select distinct
    toDate(time) as date,
    user_id,
    os
    from simulator.message_actions
    where toDate(time) between today() - 8 and today() - 1
    ) as t
    group by date
    order by date''').df
    
    # соберем датасет только с новыми пользователями
    data_new_users = Getch('''
    select date,
    uniqExact(user_id) as new_users,
    uniqExactIf(user_id, source='ads') as new_users_ads,
    uniqExactIf(user_id, source='organic') as new_users_organic
    from (
    select user_id,
    source,
    min(min_dt) as date
    from (
    select user_id,
    min(toDate(time)) as min_dt,
    source
    from simulator.feed_actions
    where toDate(time) between today() - 90 and today() - 1
    group by user_id, source
    union all
    select
    user_id,
    min(toDate(time)) as min_dt,
    source
    from simulator.message_actions
    where toDate(time) between today() - 90 and today() - 1
    group by user_id, source
    ) as t
    group by user_id, source
    ) as tab
    where date = yesterday()
    group by date''').df
    
    
    # установим форматы дат
    today = pd.Timestamp('now') - pd.DateOffset(days=1)
    
    data_feed['date'] = pd.to_datetime(data_feed['date']).dt.date
    data_msgs['date'] = pd.to_datetime(data_msgs['date']).dt.date
    data_dau_all['date'] = pd.to_datetime(data_dau_all['date']).dt.date
    data_new_users['date'] = pd.to_datetime(data_new_users['date']).dt.date
    
    # преобразуем типы данных
    data_feed = data_feed.astype({'dau_feed' : int, 'views' : int, 'likes' : int, 'events' : int, 'posts' : int})
    data_msgs = data_msgs.astype({'dau_msgs' : int, 'msgs' : int})
    data_dau_all = data_dau_all.astype({'users' : int, 'users_ios' : int, 'users_android' : int})
    data_new_users = data_new_users.astype({'new_users' : int, 'new_users_ads' : int, 'new_users_organic' : int})
    
    # текствовое сообщение для отправки в telegram
    message = ("Отчет по всему приложению за {}: \
    \n\nEvents: {:,d} \
    \nDAU: {:,d} \
    \nDAU iOS: {:,d} \
    \nDAU Android: {:,d} \
    \nНовые пользователи: {:,d} \
    \nНовые рекламные пользователи: {:,d} \
    \nНовые органические пользователи: {:,d} \
    \
    \n\nЛЕНТА: \
    \nDAU: {:,d} \
    \nКоличество просмотров: {:,d} \
    \nКоличество лайков: {:,d} \
    \nCTR: {} \
    \nКоличество опубликованных постов: {:,d} \
    \nКоличество лайков на одного пользователя: {} \
    \
    \n\nМЕССЕНДЖЕР: \
    \nDAU {:,d} \
    \nКоличество сообщений: {:,d} \
    \nКоличество сообщений на одного пользователя: {} \
    \n\nГрафики с основным метрикам: " \
                .format(today.date(),
                        data_msgs[data_msgs['date'] == today.date()]['msgs'].iloc[0] + 
                        data_feed[data_feed['date'] == today.date()]['events'].iloc[0],
                        data_dau_all[data_dau_all['date'] == today.date()]['users'].iloc[0],
                        data_dau_all[data_dau_all['date'] == today.date()]['users_ios'].iloc[0],
                        data_dau_all[data_dau_all['date'] == today.date()]['users_android'].iloc[0],
                        data_new_users[data_new_users['date'] == today.date()]['new_users'].iloc[0],
                        data_new_users[data_new_users['date'] == today.date()]['new_users_ads'].iloc[0],
                        data_new_users[data_new_users['date'] == today.date()]['new_users_organic'].iloc[0],
                        data_feed[data_feed['date'] == today.date()]['dau_feed'].iloc[0],
                        data_feed[data_feed['date'] == today.date()]['views'].iloc[0],
                        data_feed[data_feed['date'] == today.date()]['likes'].iloc[0],
                        data_feed[data_feed['date'] == today.date()]['CTR'].iloc[0],
                        data_feed[data_feed['date'] == today.date()]['posts'].iloc[0],
                        data_feed[data_feed['date'] == today.date()]['lpu'].iloc[0],
                        data_msgs[data_msgs['date'] == today.date()]['dau_msgs'].iloc[0],
                        data_msgs[data_msgs['date'] == today.date()]['msgs'].iloc[0],
                        data_msgs[data_msgs['date'] == today.date()]['mpu'].iloc[0]
                       ))
    
    plot_objects = get_plot(data_feed, data_msgs, data_new_users, data_dau_all)
    bot.sendMessage(chat_id=chat_id, text=message)
    
    for plot_object in plot_objects:
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)