# -*-coding:utf-8-*-
from reportlab.pdfbase.pdfmetrics import registerFont, registerFontFamily
from reportlab.pdfbase.ttfonts import TTFont

__all__ = ['register_cn_fonts']


def register_cn_fonts():
    """
    这是一个注册字体的方法
    """
    path = "./FontLibrary/"
    registerFont(TTFont('Deng', path + 'Deng.ttf'))
    registerFont(TTFont('Dengb', path + 'Dengb.ttf'))
    registerFont(TTFont('Dengl', path + 'Dengl.ttf'))
    registerFont(TTFont('Song', path + 'Simsun.ttf'))

    registerFont(TTFont('等线', path + 'Deng.ttf'))
    registerFont(TTFont('等线粗', path + 'Dengb.ttf'))
    registerFont(TTFont('等线细', path + 'Dengl.ttf'))
    registerFont(TTFont('宋体', path + 'Simsun.ttf'))

    registerFontFamily('Deng',
                       normal='Deng',
                       bold='Dengb',
                       italic='Dengl',
                       boldItalic='Deng')

    registerFontFamily('等线',
                       normal='等线',
                       bold='等线粗',
                       italic='等线细',
                       boldItalic='等线')
