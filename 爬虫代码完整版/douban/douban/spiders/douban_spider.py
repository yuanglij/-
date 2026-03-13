import json
import re
import scrapy
from lxml import etree
from loguru import logger
from scrapy import cmdline
from urllib.parse import urlencode
from bs4 import BeautifulSoup


class DoubanSpiderSpider(scrapy.Spider):
    name = "douban_spider"
    allowed_domains = ["movie.douban.com"]
    start_urls = ["https://movie.douban.com/subject/36469477", 'https://movie.douban.com/subject/35883131']

    def parse(self, response, **kwargs):
        movie_item = dict()
        url = response.url
        movie_id = url.split('subject/')[-1][:-1]
        movie_item['type'] = "movie"
        movie_item['movie_id'] = movie_id
        movie_item['movie_href'] = url
        tree = etree.HTML(response.text)
        # 电影评分
        score = tree.xpath('//strong[@property="v:average"]')
        if score:
            movie_item['score'] = str(score[0].text)

        # 电影名称
        movie_name = tree.xpath('//span[@property="v:itemreviewed"]/text()')
        if movie_name:
            movie_item["movie_name"] = str(movie_name[0])

        # 上映日期
        movie_release_date = tree.xpath('//span[@property="v:initialReleaseDate"]/text()')
        if movie_release_date:
            # if len(movie_release_date) > 1:
            #     movie_item['release_date'] = "/".join([str(date).strip() for date in movie_release_date])
            # else:
            #     movie_item["release_date"] = str(movie_release_date[0]).strip()
            movie_item['release_date'] = str(movie_release_date[0]).split('(')[0]
        else:
            movie_item["release_date"] = ""

        # 导演
        movie_director = tree.xpath('//span[@class="pl"][text()="导演"]/following-sibling::span[@class="attrs"]/a/text()')
        if movie_director:
            if len(movie_director) > 1:
                movie_item["directors"] = "|".join([str(attr) for attr in movie_director])
            else:
                movie_item["directors"] = str(movie_director[0])
        else:
            movie_item["directors"] = ""

        # 演员 主演
        actors_name = tree.xpath('//a[@rel="v:starring"]/text()')
        if actors_name:
            if len(actors_name) > 1:
                movie_item["actors"] = "/".join([str(attr) for attr in actors_name])
            else:
                movie_item["actors"] = str(actors_name[0])
        else:
            movie_item["actors"] = ""

        # 影片简介
        movie_introduction = tree.xpath('//span[@property="v:summary"]/text()')
        if movie_introduction:
            if len(movie_introduction) > 1:
                movie_item["storyline"] = "\n".join([str(attr).strip() for attr in movie_introduction])
            else:
                movie_item["storyline"] = str(movie_introduction[0]).strip()
        else:
            movie_item["storyline"] = ""

        # imdb_id
        movie_imdb = tree.xpath('//span[contains(text(), "IMDb:")]/following-sibling::text()[1]')
        if movie_imdb:
            movie_item["imdb_id"] = str(movie_imdb[0]).strip()
        else:
            movie_item["imdb_id"] = ""
        yield movie_item

        # 短评
        for i in range(0, 15):
            url = f"https://movie.douban.com/subject/{movie_id}/comments"
            start = i * 20
            limit = 20 + (i * 20)
            params = {
                "percent_type": "",
                "start": start,
                "limit": limit,
                "status": "P",
                # "sort": "time",   # 最新
                "sort": "new_score",  # 热门
                "comments_only": "1",
                "ck": "qnU2"
            }
            comment_url = url + "?" + urlencode(params)
            yield scrapy.Request(comment_url, method="GET", callback=self.parse_comments, cb_kwargs={
                'movie_id': movie_id,
                "movie_name": movie_item["movie_name"],
            })

        # 影评
        for i in range(0, 15):
            review_url = f"https://movie.douban.com/subject/{movie_id}/reviews"
            params = {
                "sort": "hotest",
                "start": f"{str(20 * i)}"
            }
            review_url = review_url + "?" + urlencode(params)
            yield scrapy.Request(review_url, method="GET", callback=self.parse_reviews, cb_kwargs={
                'movie_id': movie_id,
                "movie_name": movie_item["movie_name"],
            })

    def parse_comments(self, response, movie_id, movie_name):
        text_res = response.text
        json_res = json.loads(text_res)
        html = json_res["html"]
        parser = etree.HTMLParser()
        tree = etree.fromstring(html, parser)
        # 解析评论项
        comment_items = tree.xpath('//div[@class="comment-item"]')
        # 异常情况，翻到第五六页的时候突然说没有评论数据了，再继续往后翻又有数据了，所以这里需要单独处理一下
        comment_items_text = str(tree.xpath('//div[@class="comment-item"]/text()')[0]).strip() if tree.xpath('//div[@class="comment-item"]/text()') else ""

        if comment_items:
            if comment_items_text:
                logger.warning(f"comment_items_text--> {comment_items_text}")
            else:
                for item in comment_items:
                    # comment_item = LatestMovieShortReviewItem()
                    comment_item = dict()
                    comment_item['type'] = "comments"
                    comment_item['comment_id'] = str(item.xpath('./@data-cid')[0])
                    comment_item['movie_id'] = movie_id
                    comment_item['movie_name'] = movie_name
                    user_url = item.xpath('./div[@class="avatar"]/a/@href')[0]
                    comment_item['user_id'] = user_url.split('/')[-2]
                    comment_item['user_name'] = str(item.xpath('.//div[@class="avatar"]/a/@title')[0]) if item.xpath('.//div[@class="avatar"]/a/@title') else ""
                    comment_item['user_avatar'] = str(item.xpath('./div[@class="avatar"]/a/img/@src')[0]) if item.xpath('./div[@class="avatar"]/a/img/@src') else ""
                    comment_item['comment_link'] = str(item.xpath('//div[@class="comment-report"]/@data-url')[0]) if item.xpath('//div[@class="comment-report"]/@data-url') else ""
                    comment_item['comment_time'] = str(item.xpath('.//span[@class="comment-time"]/text()')[0]).strip()
                    comment_item['comment_location'] = str(item.xpath('.//span[@class="comment-location"]/text()')[0]) if item.xpath('.//span[@class="comment-location"]/text()') else ""

                    rating = str(item.xpath(".//span[text()='看过']/following-sibling::span[not(@class='comment-time')][1]/@title")[0]) if item.xpath(".//span[text()='看过']/following-sibling::span[not(@class='comment-time')][1]/@title") else ""
                    if rating == "力荐":
                        comment_item['comment_star_rating'] = 5
                    elif rating == "推荐":
                        comment_item['comment_star_rating'] = 4
                    elif rating == "还行":
                        comment_item['comment_star_rating'] = 3
                    elif rating == "较差":
                        comment_item['comment_star_rating'] = 2
                    elif rating == "很差":
                        comment_item['comment_star_rating'] = 1
                    else:
                        comment_item['comment_star_rating'] = 0
                    comment_item['content'] = item.xpath('.//p[@class="comment-content"]/span[@class="short"]/text()')[0].strip() if item.xpath('.//p[@class="comment-content"]/span[@class="short"]/text()') else ""
                    # logger.success(f"comment_item--> {comment_item}")
                    yield comment_item

    def parse_reviews(self, response, movie_id, movie_name):
        tree = etree.HTML(response.text)
        reviews = tree.xpath('//div[@class="review-list  "]/div')
        for review in reviews:
            review_item = dict()
            review_item['type'] = "reviews"
            review_id = review.attrib['data-cid']
            review_item['review_id'] = review_id
            user_url = review.xpath('.//a[@class="avator"]/@href')[0]
            review_item['user_name'] = str(review.xpath('.//a[@class="name"]/text()')[0])
            review_item['movie_id'] = movie_id
            review_item['user_id'] = user_url.split('/')[-2]
            review_item['date'] = str(review.xpath('.//span[@class="main-meta"]/text()')[0])
            review_url = f"https://movie.douban.com/j/review/{review_id}/full"
            yield scrapy.Request(review_url, method="GET", callback=self.parse_full_review, cb_kwargs=review_item)

    def parse_full_review(self, response, type, review_id, user_name, movie_id, user_id, date):
        review_item = dict()
        review_item['type'] = type
        review_item['review_id'] = review_id
        review_item['user_name'] = user_name
        review_item['movie_id'] = movie_id
        review_item['user_id'] = user_id
        review_item['date'] = date
        text_res = json.loads(response.text)
        html = text_res['html']
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        content = text.strip()
        review_item['content'] = content
        yield review_item


if __name__ == '__main__':
    cmdline.execute("scrapy crawl douban_spider".split())
