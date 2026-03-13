# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import os


class DoubanPipeline:
    # 连接数据库
    def __init__(self):
        self.connect = pymysql.Connect(host="localhost", user="root", password="123456", port=3306, db="douban", charset="utf8mb4")
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        download_path = os.getcwd() + '/download/'
        if not os.path.exists(download_path):
            os.mkdir(download_path)

        _type = item.get('type')
        if _type == 'movie':
            movie_id = item.get('movie_id')
            movie_name = item.get('movie_name')
            score = item.get('score')
            movie_release_date = item.get('release_date')
            movie_href = item.get('movie_href')
            directors = item.get('directors')
            actors = item.get('actors')
            storyline = item.get('storyline')
            # print(movie_id, movie_name, movie_score, movie_release_date, movie_href, directors, actors, storyline)
            # 插入数据
            sql = "INSERT INTO movie (movie_id, movie_name, score, release_date, movie_href, directors, actors, storyline) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(sql, (movie_id, movie_name, score, movie_release_date, movie_href, directors, actors, storyline))
            self.connect.commit()

        if _type == 'comments':
            movie_id = item.get('movie_id')
            movie_name = item.get('movie_name')
            comment_id = item.get('comment_id')
            user_name = item.get('user_name')
            user_avatar = item.get('user_avatar')
            comment_link = item.get('comment_link')
            comment_time = item.get('comment_time')
            comment_star_rating = item.get('comment_star_rating')
            content = item.get('content')
            # 插入数据
            sql = "INSERT INTO comments (comment_id, movie_id, user_name, user_avatar, comment_link, comment_time, comment_star_rating, content) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(sql, (comment_id, movie_id, user_name, user_avatar, comment_link, comment_time, comment_star_rating, content))
            self.connect.commit()
            # print(movie_id, movie_name, comment_id, user_name, user_avatar, comment_link, comment_time, comment_star_rating, content)

        if _type == 'reviews':
            review_id = item.get('review_id')
            user_name = item.get('user_name')
            movie_id = item.get('movie_id')
            user_id = item.get('user_id')
            date = item.get('date')
            content = item.get('content')
            # 插入数据
            sql = "INSERT INTO reviews (review_id, user_name, movie_id, user_id, date, content) VALUES (%s, %s, %s, %s, %s, %s)"
            self.cursor.execute(sql, (review_id, user_name, movie_id, user_id, date, content))
            self.connect.commit()
            # print(review_id, user_name, movie_id, user_id, date, content)

    # 关闭资源
    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()
