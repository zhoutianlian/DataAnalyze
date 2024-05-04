from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.colors import Color
import matplotlib.pyplot as plt
from matplotlib import image  # 仅用于测试在线渲染图片的功能
import io
from CreateStockIndex.pypdf_v2.Elements.Document import Document

# pip install -i https://pypi.tuna.tsinghua.edu.cn/simple

# create pdf
a = Document('output', 'test.pdf')

# craete new page
a.new_page('cover')

# insert background
a.add_image('Resource/bg.png', (0, 0), (1, 1), )

# add square
a.add_rect((0.08, 0.04), (0.84, 0.92), stroke=0, fill=1, fill_color=(0.1, 0.1, 0.25, 0.85))
# add title
a.add_title((0.11, 0.9), '富由科技50指数', font_name='等线', font_color=(1, 1, 1, 1), font_size=22, align='left')
# add subtitle
a.add_title((0.11, 0.87), '每日行情简报', font_name='等线', font_color=(1, 1, 1, 1), font_size=14, align='left')  # 绘制副标题

today = datetime.today()
year, month, day = today.year, today.month, today.day
a.add_title((0.11, 0.845), '%04d-%02d-%02d' % (year, month, day), font_name='等线', font_color=(1, 1, 1, 1),
            font_size=12, align='left')
# 绘制副标题：时间

performance = [['起始日期', '2020-11-01', '起始点数', '1000.00'],
               ['最新日期', '2021-02-05', '最新点数', '1531.27'],
               ['累计涨跌幅', '53.13%', '最大回撤', '-8.28%'],
               ['日均涨跌幅', '1.13%', '日波动率', '1.25%'],
               ['周均涨跌幅', '4.25%', '周波动率', '5.58%'],
               ['52周高', '1830.53', '52周低', '993.15']]

table_style = [('FONTNAME', (0, 0), (-1, -1), '等线'),
               ('TEXTCOLOR', (0, 0), (-1, -1), colors.white),
               ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
               ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
               ('LEFTPADDING', (0, 0), (-1, -1), 30),
               ('RIGHTPADDING', (0, 0), (-1, -1), 30),
               # ('GRID', (0, 0), (-1, -1), 0.5, colors.white),
               ('ROWBACKGROUNDS', (0, 0), (-1, -1), [Color(0, 0, 139, 0.3), Color(139, 28, 98, 0.3)]),]
a.add_table(performance, (0.14, 0.7), (0.72, 0.12), style=table_style)

img = image.imread('Resource/dk.jpg')

fig = plt.figure(figsize=(16, 9))
plt.imshow(img)
plt.xticks([])
plt.yticks([])
img_io = io.BytesIO()
plt.savefig(img_io, format='PNG', transparent=True, bbox_inches='tight', dpi=600)

a.add_image(img_io, (0.14, 0.38), (0.72, 0.3))

a.add_title((0.5, 0.06), '———————— 富由基金 ————————', font_color=(1, 1, 1, 1), font_size=12, align='center')

a.publish()
