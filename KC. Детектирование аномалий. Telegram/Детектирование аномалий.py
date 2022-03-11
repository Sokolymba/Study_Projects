# импортируем библиотеки
import pandas as pd
import numpy as np
import telegram
from statistics import mean
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import FuncFormatter
import locale
import io
import os
import seaborn as sns
from read_db.CH import Getch

# установим параметры для графиков, а также ссылки на общий дашборд и каждый график в superset

# запускаем бота и прописываем chat_id
bot = telegram.Bot(token = '2146781779:AAEp2ClBZ1g39sUCOyDS5hQx7asLGBgtsbY')
chat_id = 221427850

# импортируем из Clickhouse данные по основным метрикам для всего приложения с разбивкой по 15-минуткам
data = Getch("select toTime(time) as time, dau_feed, views, likes, ctr, dau_msgs, messages \
from \
(select toStartOfFifteenMinutes(time) as time, \
uniq(user_id) as dau_feed, \
countIf(action = 'view') as views, \
countIf(action = 'like') as likes, \
round((countIf(action = 'like') / countIf(action = 'view') * 100), 1) as ctr \
from simulator.feed_actions \
where time >= toDate(toStartOfFifteenMinutes(now())-1) and time <= toStartOfFifteenMinutes(now())-1 \
group by time \
order by time) as t1 \
left join \
(select toTime(time) as time, dau_msgs, messages \
from \
(select toStartOfFifteenMinutes(time) as time, \
uniq(user_id) as dau_msgs, \
count(reciever_id) as messages \
from simulator.message_actions \
where time >= toDate(toStartOfFifteenMinutes(now())-1) and time <= toStartOfFifteenMinutes(now())-1 \
group by time \
order by time \
)) as t2 \
using time").df

# импортируем из Clichouse данные за 7 дней и посчитаем нижний, и верхний перцентили для определения границ доверительного интервала
data_quan = Getch("select toTime(time) as time, anyLast(dau_feed) as yd_dau_feed, quantileExactExclusive(0.25)(dau_feed) as low_dau_feed, quantileExactExclusive(0.75)(dau_feed) as high_dau_feed, \
    anyLast(views) as yd_views, quantileExactExclusive(0.25)(views) as low_views, quantileExactExclusive(0.75)(views) as high_views, \
    anyLast(likes) as yd_likes, quantileExactExclusive(0.25)(likes) as low_likes, quantileExactExclusive(0.75)(likes) as high_likes, \
    anyLast(ctr) as yd_ctr, quantileExactExclusive(0.25)(ctr) as low_ctr, quantileExactExclusive(0.75)(ctr) as high_ctr, \
    anyLast(dau_msgs) as yd_dau_msgs, quantileExactExclusive(0.25)(dau_msgs) as low_dau_msgs, quantileExactExclusive(0.75)(dau_msgs) as high_dau_msgs, \
    anyLast(messages) as yd_messages, quantileExactExclusive(0.25)(messages) as low_messages, quantileExactExclusive(0.75)(messages) as high_messages \
from (select * from \
(select toStartOfFifteenMinutes(time) as time, uniq(user_id) as dau_feed, \
countIf(action='view') as views, \
countIf(action='like') as likes, \
round(likes/views*100, 1) as ctr \
from simulator.feed_actions \
where time >= today()-7 and toTime(time) <= toTime(toStartOfFifteenMinutes(now())-1) and time < toDate(toStartOfFifteenMinutes(now())-1) \
group by time) as t1 \
left join \
(select toStartOfFifteenMinutes(time) as time, \
uniq(user_id) as dau_msgs, \
count(user_id) as messages \
from simulator.message_actions \
where time >= today()-7 and toTime(time) <= toTime(toStartOfFifteenMinutes(now())-1) and time < toDate(toStartOfFifteenMinutes(now())-1) \
group by time) as t2 \
using time \
order by toTime(time), toDate(time) asc) \
group by time \
order by time asc").df

# напишем цикл для сборки и отправки алерта, и графиков
def alert(df, plot):
    coef = 1.5
    iqr = plot.iloc[:, 2] - plot.iloc[:, 1]
    iqr_25 = plot.iloc[:, 1] - iqr * coef
    iqr_75 = plot.iloc[:, 2] + iqr * coef
    
    # зададим условия для подстановки метрик и графиков из Superset 
    if df.name == 'dau_feed':
        chart = 'http://superset.lab.karpov.courses/r/498'
        met='Пользователи ленты'
    elif df.name == 'views':
        chart = 'http://superset.lab.karpov.courses/r/501'
        met='Просмотры ленты'
    elif df.name == 'likes':
        chart = 'http://superset.lab.karpov.courses/r/500'
        met='Лайки ленты'
    elif df.name == 'ctr':
        chart = 'http://superset.lab.karpov.courses/r/499'
        met='CTR ленты'
    elif df.name == 'dau_msgs':
        chart = 'http://superset.lab.karpov.courses/r/502'
        met='Пользователи сообщений'
    elif df.name == 'messages':
        chart = 'http://superset.lab.karpov.courses/r/503'
        met='Количество сообщений'
    else:
        chart = "https://superset.lab.karpov.courses/superset/dashboard/167/"
        met='Метрика'
        
    # напишем условие, при котором чат-бот telegram будет присылать нам алерт
    if df.iloc[-1] < iqr_25.iloc[-1] or df.iloc[-1] > iqr_72.iloc[-1]:
        
        # текстовое сообщение алерта
        alert = ("Аномалия в метрике {} в срезе {} \n\nТекущее значение: {:,d}. Отклонение более {}% \n\nСсылка на риалтайм чарт: {} \n\nСсылка на риалтайм дашборд: {} \n\n@Sokolymba, посмотри, пожалуйста, на "\
                 .format(met, df.index[-1].strftime('%H:%M'), df.iloc[-1], round((df.iloc[-1]-graph.iloc[-1:, 1]\
                                                                                  .values[0])/graph.iloc[-1:, 1]\
                                                                                 .values[0]*100,1), chart, url_dash))
        
        # отправляем текст
        bot.sendMessage(chat_id=chat_id, text=alert)
        
        # параметры для графиков
        plt.title('График метрики')
        sns.lineplot(data=df, color='pink')
        sns.lineplot(data=plot.iloc[:, 0], linestyle="dashed")
        plt.fill_between(x=plot.index, y1=iqr_25, y2=iqr_75, alpha=.3)
        plt.xlabel('Дата')
        plt.ylabel('Значение метрики')
        plt.tight_layout()

        # сохраняем график в png
        plot_object_alert = io.BytesIO()
        plt.savefig(plot_object_alert)
        plot_object_alert.name = 'alert_plot.png'
        plot_object_alert.seek(0)
        
        plt.close()
        
        # отправляем график
        bot.sendPhoto(chat_id = chat_id, photo = plot_object_users)