# -*- coding: utf-8 -*-
# 此程序用来抓取芒果视频的数据
import csv

import requests
import time
import random
import re
from fake_useragent import UserAgent, FakeUserAgentError
from multiprocessing.dummy import Pool
from datetime import datetime
from datetime import timedelta
from save_data import database

class Spider(object):
	def __init__(self):
		try:
			self.ua = UserAgent(use_cache_server=False).random
		except FakeUserAgentError:
			pass
		# self.date = '2000-01-01'
		# self.limit = 50000
		self.year, self.month, self.day = time.strftime("%Y-%m-%d", time.localtime(time.time())).split('-')
		self.db = database()
		
	def get_headers(self):
		# user_agent = self.ua.chrome
		user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
					   'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
					   'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
					   'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
					   'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
					   'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)',
					   'Opera/9.52 (Windows NT 5.0; U; en)',
					   'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.2pre) Gecko/2008071405 GranParadiso/3.0.2pre',
					   'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.458.0 Safari/534.3',
					   'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.4 Safari/532.0',
					   'Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.7.39 Version/11.00']
		user_agent = random.choice(user_agents)
		headers = {'host': "pcweb.api.mgtv.com",
		           'connection': "keep-alive",
		           'user-agent': user_agent,
		           'accept': "*/*",
		           'referer': "https://www.mgtv.com/h/321787.html?fpa=se",
		           'accept-encoding': "gzip, deflate",
		           'accept-language': "zh-CN,zh;q=0.9"
		           }
		return headers
	
	def p_time(self, stmp):  # 将时间戳转化为时间
		stmp = float(str(stmp)[:10])
		timeArray = time.localtime(stmp)
		otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
		return otherStyleTime
	
	def replace(self, x):
		# 将其余标签剔除
		removeExtraTag = re.compile('<.*?>', re.S)
		x = re.sub(removeExtraTag, "", x)
		x = re.sub('/', ";", x)
		x = re.sub(re.compile('\s{2,}'), ' ', x)
		x = re.sub('[\n\r]', ' ', x)
		return x.strip()
	
	def GetProxies(self):
		# 代理服务器
		proxyHost = "http-dyn.abuyun.com"
		proxyPort = "9020"
		# 代理隧道验证信息
		proxyUser = "HI18001I69T86X6D"
		proxyPass = "D74721661025B57D"
		proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
			"host": proxyHost,
			"port": proxyPort,
			"user": proxyUser,
			"pass": proxyPass,
		}
		proxies = {
			"http": proxyMeta,
			"https": proxyMeta,
		}
		return proxies

	def get_film_id(self, film_url):  # 获取电视剧每一集的id
		url = "https://pcweb.api.mgtv.com/episode/list"
		p = re.compile('h/(\d+?)\.')
		collection_id = re.findall(p, film_url)[0]
		total_page = 0
		page = 0
		results = []
		retry = 5
		while page <= total_page:
			while 1:
				try:
					querystring = {"collection_id": collection_id, "page": str(page), "size": "25",
					               "_support": "10000000"}
					text = requests.get(url, params=querystring, proxies=self.GetProxies(), headers=self.get_headers(),
					                    timeout=10).json()
					break
				except Exception as e:
					retry -= 1
					if retry == 0:
						print e
						return None
					else:
						continue
			total_page = text['data']['total_page'] - 1
			items = text['data']['list']
			for item in items:
				video_id = str(item['video_id'])
				results.append(video_id)
			page += 1
		return results
	
	def get_detail_comments_pagenums(self, videoid):  # 获取某一个视频的总评论页数
		url = "https://comment.mgtv.com/v4/comment/getCommentList"
		querystring = {"page": "1", "subjectType": "hunantv2014", "subjectId": videoid}
		retry = 5
		while 1:
			try:
				headers = self.get_headers()
				headers['host'] = 'comment.mgtv.com'
				total = \
					requests.get(url, headers=headers, proxies=self.GetProxies(), params=querystring,
					             timeout=10).json()[
						'data'][
						'commentCount']
				# if total > self.limit:
				# 	total = self.limit
				if total % 15 == 0:
					pagenums = total / 15
				else:
					pagenums = total / 15 + 1
				return pagenums
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
	
	def get_detail_comments_page(self, ss):  # 获取某一页的所有评论
		film_url, videoid, product_number, plat_number, page = ss
		print product_number +'|'+videoid+ '|' + str(page)
		url = "https://comment.mgtv.com/v4/comment/getCommentList"
		querystring = {"page": page, "subjectType": "hunantv2014", "subjectId": videoid}
		retry = 10
		while 1:
			try:
				headers = self.get_headers()
				headers['host'] = 'comment.mgtv.com'
				results = []
				items = \
					requests.get(url, headers=headers, params=querystring, proxies=self.GetProxies(),
					             timeout=10).json()[
						'data']['list']
				last_modify_date = self.p_time(time.time())
				for item in items:
					try:
						nick_name = item['user']['nickName']
					except:
						nick_name = ''
					cmt_date = self.get_date(item['date'])
					# if cmt_date < self.date:
					# 	continue
					cmt_time = cmt_date + ' 00:00:00'
					try:
						comments = self.replace(item['content'])
					except:
						comments = ''
					try:
						like_cnt = str(item['praiseNum'])
						if len(like_cnt) == 0:
							like_cnt = '0'
					except:
						like_cnt = '0'
					try:
						cmt_reply_cnt = str(item['commentNum'])
						if len(cmt_reply_cnt) == 0:
							cmt_reply_cnt = '0'
					except:
						cmt_reply_cnt = '0'
					long_comment = '0'
					source_url = film_url
					tmp = [product_number, plat_number, nick_name, cmt_date, cmt_time, comments, like_cnt,
					       cmt_reply_cnt, long_comment, last_modify_date, source_url]
					print '|'.join(tmp)
					results.append([x.encode('gbk', 'ignore') for x in tmp])
				return results
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
	
	def get_date_n(self, days):
		date = datetime.now() - timedelta(days=days)
		return date.strftime('%Y-%m-%d')
	
	def get_date(self, orgin_date):  # 对date进行处理
		date = re.sub(u'年', '-', orgin_date)
		date = re.sub(u'月', '-', date)
		date = re.sub(u'日', '', date)
		if u'年' in orgin_date:
			return date
		elif u'前' in orgin_date:
			return self.year + '-' + self.month + '-' + self.day
		elif u'内' in orgin_date:
			return self.year + '-' + self.month + '-' + self.day
		elif u'天前' in orgin_date:
			p = re.compile('(\d+)')
			n = re.findall(p, orgin_date)[0]
			return self.get_date_n(int(n))
		else:
			t = self.month + '-' + self.day
			if date > t:
				return str(int(self.year) - 1) + '-' + date
			else:
				return self.year + '-' + date
	
	def save_sql(self, table_name,items):  # 保存到sql
		all = len(items)
		print all
		results = []
		for i in items:
			try:
				t = [x.decode('gbk', 'ignore') for x in i]
				dict_item = {'product_number': t[0],
				             'plat_number': t[1],
				             'nick_name': t[2],
				             'cmt_date': t[3],
				             'cmt_time': t[4],
				             'comments': t[5],
				             'like_cnt': t[6],
				             'cmt_reply_cnt': t[7],
				             'long_comment': t[8],
				             'last_modify_date': t[9],
				             'src_url': t[10]}
				results.append(dict_item)
			except:
				continue
		for item in items:
			try:
				self.db.add(table_name, item)
			except:
				continue
	
	def get_all_comments(self, film_url, videoid, product_number, plat_number):  # 获取某个视频的所有评论
		pagenums = self.get_detail_comments_pagenums(videoid)
		if pagenums is None:
			return None
		else:
			ss = []
			print 'pagenums:%s' % pagenums
			for page in range(1, int(pagenums) + 1):
				ss.append([film_url, videoid, product_number, plat_number, page])
			pool = Pool(10)
			items = pool.map(self.get_detail_comments_page, ss)
			pool.close()
			pool.join()
			mm = []
			for item in items:
				if item is not None:
					mm.extend(item)
			'''
			with open('data_mgtv.csv', 'a') as f:
				writer = csv.writer(f, lineterminator='\n')
				writer.writerows(mm)
			'''
			# self.save_sql('t_comments_pub', mm)  # 手动修改需要录入的库的名称
			print u'%s 开始录入数据库' % product_number
			self.save_sql('T_COMMENTS_PUB', mm)  # 手动修改需要录入的库的名称
			print u'%s 录入数据库完毕' % product_number
	
	def get_all(self, film_url, product_number, plat_number):  # 根据url获取电视剧或者电影的所有结果
		if '/h/' in film_url:
			videoids = self.get_film_id(film_url)
			if videoids is None:
				return None
			else:
				for videoid in videoids:
					print videoid
					self.get_all_comments(film_url, videoid, product_number, plat_number)
		else:
			p0 = re.compile('/(\d+?)\.html')
			videoid = re.findall(p0, film_url)[0]
			self.get_all_comments(film_url, videoid, product_number, plat_number)


if __name__ == "__main__":
	spider = Spider()
	s = []
	with open('data.csv') as f:
		tmp = csv.reader(f)
		for i in tmp:
			if 'http' in i[2]:
				s.append([i[2], i[0], 'P07'])
	for j in s:
		print j[1]
		spider.get_all(j[0], j[1], j[2])
	spider.db.db.close()
