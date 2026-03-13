# 文本情感分析
网络数据采集-爬虫-情感分析

对评论进行文本情感分析:
分别使用了TF-IDF和词向量word2vec方法分析豆瓣评论
首先进行文本清洗：去除表情符号、链接、乱码、空格等无关字符，保留纯文本内容。
然后使用jieba分词工具将评论拆分为词语，并过滤停用词（如“的”“了”“啊”等无意义词），使用模型
（“https://huggingface.co/Ayazhankad/bert-finetuned-semantic-chinese”）
对评论正向&负向进行标注并输出得到“comments_with_sentiment.xlsx”

TF-IDF：定义并训练 MLPClassifier（多层感知机）评估结果
词向量word2vec：利用训练好的词向量模型
（“douban-movie-1000w-Word2Vec.200.15.bin”）
训练MLP模型并对模型进行评估
