from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfgen import canvas
from reportlab.platypus import Table, TableStyle
from reportlab.lib.utils import ImageReader
import numpy as np
import pandas as pd
import os
import io
from FontLibrary.Font import register_cn_fonts as rcf

pdfmetrics.registerFont(UnicodeCIDFont('STSong-Light'))


class Document:
	def __init__(self, save_path: str, save_name: str, page_size: tuple = A4):
		self._c = None
		self._page = None
		rcf()

		if not os.path.exists(save_path):
			os.makedirs(save_path)

		assert save_name.endswith('.pdf')
		self._file = save_path + '/' + save_name

		assert len(page_size) == 2, 'Pagesize should have 2 dimensions: (width, height).'
		assert min(page_size) > 0
		self._page_size = page_size
		self._shape = np.array(page_size)

	def new_page(self, page_name: str = None):
		if self._page is None:
			self._c = canvas.Canvas(self._file, self._page_size)
			self._c.setFont('STSong-Light', 16)

		else:
			self._c.showPage()

		self._page = page_name

	@property
	def now_page(self):
		return self._page

	def _ensure(self, shapes: list = None, positives: list = None, non_negative: list = None):
		assert self._c is not None, 'Please start a new page first!'
		if shapes is not None:
			for i in shapes:
				if i is not None:
					assert len(i) == 2, 'Dimension error!'
		if positives is not None:
			for i in positives:
				if i is not None:
					assert min(i) > 0, 'Positive error!'
		if non_negative is not None:
			for i in non_negative:
				if i is not None:
					assert min(i) >= 0, 'Non negative error!'

	@staticmethod
	def _general_transform(shape, target, in_pct):
		if in_pct:
			return np.array(shape) * target
		else:
			return shape

	def add_title(self, loc: tuple, text: str, in_pct: bool = True, font_name='等线', font_color=(0, 0, 0, 1),
	              font_size=16, align='left', **kwargs):
		self._ensure([loc], [loc])
		self._c: canvas.Canvas
		x, y = self._general_transform(loc, self._shape, in_pct)
		self._c.setFont(font_name, font_size)
		self._c.setFillColorRGB(*font_color)
		if align == 'left':
			self._c.drawString(x, y, text, **kwargs)
		elif align == 'center':
			self._c.drawCentredString(x, y, text, **kwargs)
		elif align == 'right':
			self._c.drawRightString(x, y, text, **kwargs)

	def add_paragraph(self, loc: tuple, shape: tuple, text: str, in_pct: bool = True, **kwargs):
		self._ensure([loc, shape], [loc, shape])
		self._c: canvas.Canvas
		x, y = self._general_transform(loc, self._shape, in_pct)
		w, h = self._general_transform(shape, self._shape, in_pct)

	def add_image(self, image, loc: tuple, shape: tuple, in_pct: bool = True, **kwargs):
		self._ensure([loc, shape], [shape])
		self._c: canvas.Canvas
		x, y = self._general_transform(loc, self._shape, in_pct)
		w, h = self._general_transform(shape, self._shape, in_pct)

		if type(image) is str:
			self._c.drawImage(image, x, y, w, h)
		else:
			img = ImageReader(image)
			self._c.drawImage(img, x, y, w, h)

	def add_rect(self, loc: tuple, shape: tuple, fill_color: tuple = (0, 0, 0, 0),
	             stroke_color: tuple = (1, 1, 1, 1), in_pct: bool = True, **kwargs):
		self._ensure([loc, shape], [shape])
		self._c: canvas.Canvas
		x, y = self._general_transform(loc, self._shape, in_pct)
		w, h = self._general_transform(shape, self._shape, in_pct)
		self._c.setFillColorRGB(*fill_color)
		self._c.setStrokeColorRGB(*stroke_color)
		self._c.rect(x, y, w, h, **kwargs)

	def add_table(self, data: list, loc: tuple, shape: tuple, row_heights: list = None, col_widths: list = None,
	              in_pct: bool = True, style: list = None, **kwargs):
		self._ensure([loc, shape], [shape, row_heights, col_widths])
		x, y  = self._general_transform(loc, self._shape, in_pct)
		w, h = self._general_transform(shape, self._shape, in_pct)
		row_count, col_count = len(data), len(data[0])

		if row_heights is None:
			row_heights = [h / row_count for _ in range(row_count)]
		else:
			assert len(row_heights) == row_count
			row_heights = np.array(row_heights)
			row_heights /= row_heights.sum()
			row_heights *= h
			row_heights = list(row_heights)

		if col_widths is None:
			col_widths = [w / col_count for _ in range(col_count)]
		else:
			assert len(col_widths) == col_count
			col_widths = np.array(col_widths)
			col_widths /= col_widths.sum()
			col_widths *= w
			col_widths = list(col_widths)

		t = Table(data, colWidths=col_widths, rowHeights=row_heights, style=TableStyle(style))
		t.wrap(w, h)
		t.drawOn(self._c, x, y)

	def publish(self):
		self._ensure()
		self._c.showPage()
		self._c.save()
