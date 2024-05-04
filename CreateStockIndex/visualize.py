"""
生成指数单张
"""
import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt
import time
import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import mplfinance as mpf
from cycler import cycler
import matplotlib as mpl
import pyecharts.options as opts
from pyecharts.charts import Pie,Grid, Line, Scatter
from pyecharts.charts import Line
from pyecharts import options as opts
from pyecharts.faker import Faker

# plot k line
# input dataframe: 'open', 'high', 'low', 'close', 'volume', 'trade_date'
# add name and frequency
def Kplot(quote, stock_code, frequency):
    fig_data = quote[['open', 'high', 'low', 'close', 'volume', 'trade_date']]
    fig_data.rename(
        columns={
            'trade_date': 'Date', 'open': 'Open',
            'high': 'High', 'low': 'Low',
            'close': 'Close', 'volume': 'Volume'},
        inplace=True)
    fig_data['Date'] = fig_data['Date'].map(lambda x: str(x))
    fig_data['Date'] = pd.to_datetime(fig_data['Date'])
    fig_data.set_index(['Date'], inplace=True)
    fig_data.sort_index(ascending=True, inplace=True)
    print(fig_data)

    kwargs = dict(
        type='candle',
        mav=(7, 30, 60),
        volume=True,
        title='\n %s candle_line' % (stock_code),
        ylabel='OHLC Candles',
        ylabel_lower='Shares\nTraded Volume',
        figratio=(16, 10),
        figscale=2)

    mc = mpf.make_marketcolors(
        up='#ff00ff',
        down='#00ff00',
        edge='lime',
        wick={'up': 'blue', 'down': 'orange'},
        volume='in',
        ohlc='black',
        inherit=True)
    s = mpf.make_mpf_style(
        base_mpf_style='nightclouds',
        gridaxis='both',
        gridstyle='-.',
        y_on_right=False,
        marketcolors=mc)

    # 设置均线颜色，配色表可见下图
    # 建议设置较深的颜色且与红色、绿色形成对比
    # 此处设置七条均线的颜色，也可应用默认设置
    mpl.rcParams['axes.prop_cycle'] = cycler(
        color=['dodgerblue', 'deeppink',
               'navy', 'teal', 'maroon', 'darkorange',
               'indigo'])

    mpl.rcParams['lines.linewidth'] = .5

    mpf.plot(fig_data,
             style=s,
             **kwargs,
             show_nontrading=False,
             datetime_format='%Y-%m-%d',
             tight_layout=True,
             savefig=os.path.join('%s_candle_line_%s' % (stock_code, frequency) + '.jpg'))
    plt.show()


def visualize_distribution(name, labels, sizes):
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    plt.show()
    plt.savefig(os.path.join(os.getcwd(), 'static', '%s.jpg' % (name)))


def visualize_distribution_1(name, labels, sizes):
    fig, ax = plt.subplots(figsize=(5, 5), subplot_kw=dict(aspect="equal"))
    wedges, texts = ax.pie(sizes, labels=labels,
                           textprops=dict(color="w"))

    ax.legend(wedges, labels,
              title=name,
              loc="center left",
              bbox_to_anchor=(1, 0, 0.5, 1))
    plt.savefig(os.path.join(os.getcwd(), 'static', '%s.jpg' % (name)))

    # ax.set_title("Matplotlib bakery: A pie")

    plt.show()

# html, pie
def visualize_pie(data, label, title):
    data_pair = [list(z) for z in zip(label, data)]
    data_pair.sort(key=lambda x: x[1])

    (
        Pie(init_opts=opts.InitOpts(width="800px", height="800px", theme=ThemeType.DARK))
            .add(
            series_name="访问来源",
            data_pair=data_pair,
            rosetype="radius",
            radius="55%",
            center=["50%", "50%"],
            label_opts=opts.LabelOpts(is_show=False, position="center"),
        )
            .set_global_opts(
            title_opts=opts.TitleOpts(
                title=title,
                pos_left="center",
                pos_top="20",
                title_textstyle_opts=opts.TextStyleOpts(color="#fff"),
            ),
            legend_opts=opts.LegendOpts(is_show=False),
        )
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(
                trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"
            ),
            label_opts=opts.LabelOpts(color="rgba(255, 255, 255, 0.3)"),
        )
            .render("%s.html" % (title))
    )

# html, line
def visualize_line(x,y_1,y_2,title):
    c = (
        Line(init_opts=opts.InitOpts(width="1400px", height="800px",theme=ThemeType.DARK))

        .add_xaxis(xaxis_data=x)
        .add_yaxis(series_name = '富由50', y_axis=y_1, is_smooth=True)
        .add_yaxis(series_name = "HS300",y_axis=y_2, is_smooth=True)

        .set_series_opts(
            areastyle_opts=opts.AreaStyleOpts(opacity=0.4),
            label_opts=opts.LabelOpts(is_show=False),
        )

        .set_global_opts(
            legend_opts=opts.LegendOpts(pos_right="20%",textstyle_opts=opts.TextStyleOpts(color="#fff")),
            title_opts=opts.TitleOpts(title=title,
                                     title_textstyle_opts=opts.TextStyleOpts(color="#fff"),),
            xaxis_opts=opts.AxisOpts(
                axistick_opts=opts.AxisTickOpts(is_align_with_label=True),
                is_scale=False,
                boundary_gap=False,
            ),
        )
        .render("%s.html"%(title))
    )

# html, grid

def grid(pie_label, pie_data, line_x, line_y_1, line_y_2, title):
    data_pair = [list(z) for z in zip(pie_label, pie_data)]
    data_pair.sort(key=lambda x: x[1])

    pie = (
        Pie()
            .add(
            series_name="访问来源",
            data_pair=data_pair,
            rosetype="radius",
            radius="55%",
            center=["80%", "50%"],
            label_opts=opts.LabelOpts(is_show=False, position="center"),
        )
            .set_global_opts(
            title_opts=opts.TitleOpts(
                title='industry_distribution',
                pos_right="10%",
                pos_top="20",
                title_textstyle_opts=opts.TextStyleOpts(color="#fff"),
            ),
            legend_opts=opts.LegendOpts(is_show=False),
        )
            .set_series_opts(
            tooltip_opts=opts.TooltipOpts(
                trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"
            ),
            label_opts=opts.LabelOpts(color="rgba(255, 255, 255, 0.3)"),
        )
    )

    line = (
        Line()
            .add_xaxis(xaxis_data=line_x)
            .add_yaxis(series_name='富由50', y_axis=line_y_1, is_smooth=True)
            .add_yaxis(series_name="HS300", y_axis=line_y_2, is_smooth=True)

            .set_series_opts(
            areastyle_opts=opts.AreaStyleOpts(opacity=0.4),
            label_opts=opts.LabelOpts(is_show=False),
        )

            .set_global_opts(
            legend_opts=opts.LegendOpts(pos_left="10%", textstyle_opts=opts.TextStyleOpts(color="#fff")),
            title_opts=opts.TitleOpts(pos_left="30%",
                                      title='富由50 versus HS300',
                                      pos_top="20",
                                      title_textstyle_opts=opts.TextStyleOpts(color="#fff"), ),
            xaxis_opts=opts.AxisOpts(
                axistick_opts=opts.AxisTickOpts(is_align_with_label=True),
                is_scale=False,
                boundary_gap=False,
            ),
        )
    )

    grid = (
        Grid(init_opts=opts.InitOpts(bg_color="#2c343c", width="1400px", height="600px", theme=ThemeType.DARK))
            .add(line, grid_opts=opts.GridOpts(pos_right="40%"))
            .add(pie, grid_opts=opts.GridOpts(pos_left="60%"))

            .render("%s.html" % (title))
    )


if __name__ == '__main__':

    index_quote = pd.read_csv(os.path.join(os.getcwd(), 'static', 'index_quote.csv'))
    Kplot(index_quote, 'fuyou_index', '1d')

