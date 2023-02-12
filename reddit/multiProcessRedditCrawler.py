import praw
import json
import uuid
import os
from multiprocessing import Process
from datetime import datetime
from elasticsearch import Elasticsearch, TransportError
# http_auth=('user', 'pass')
# root/ GoGetOne123$


def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())


def SingleProcessRedditCrawler(reddit, topic, subject, es_client, local_save=True):
    """
    single thread reddit crawler based on subreddit.
    :param reddit: reddit connection object
    :param topic: subreddit topics like "r/MachineLearning, r/DataScience"
    :param subject: search query for sub reddit in list format
    :param es_client: elastic search client connection
    :param local_save: Saving documents locally instead of saving to elastic search.
    :return:
    """
    #with open("{}.txt".format(topic), "w+") as df:
    subreddit = reddit.subreddit(topic)
    for q in subject:
        query = [q]
        for item in query:
            data_object = dict()
            print(reddit, topic, subject, es_client)
            for submission in subreddit.search(query, sort="new", limit=1000):
                data_object["reddit_subreddit"] = topic
                data_object["reddit_subreddit_query"] = item
                data_object["title"] = submission.title
                data_object["reddit_id"] = submission.id
                data_object["total_comments"] = submission.num_comments
                data_object["created"] = (submission.created) * 1000
                data_object["text"] = submission.selftext
                data_object["url"] = submission.url
                dt_object = datetime.fromtimestamp(submission.created)
                #index_name = "{}-{}".format(sub.lower(), dt_object.strftime('%Y-%m-%d'))
                index_name = str(topic).lower()
                submission.comments.replace_more(limit=1)
                comment_lists = list()
                for comment in submission.comments.list():
                    comment_object = dict()
                    comment_object["comment_id"] = comment.id
                    comment_object["comment_parent_id"] = comment.parent_id
                    comment_object["comment_body"] = comment.body
                    comment_object["comment_link_id"] = comment.link_id
                    comment_lists.append(comment_object)
                data_object["comments"] = comment_lists
                try:

                        es_client.index(index=index_name, doc_type='json', id=str(uuid.uuid4()),
                                        body=json.dumps(data_object), timeout="6000s")
                except TransportError as e:
                     raise ValueError("Problem in {} connection, Error is {}".format("localhost", e.message))
    #                 if not local_save:
    #                     try:
    #                         if es_client.indices.exists(index=index_name):
    #                             es_client.index(index=index_name, doc_type='json', id=str(uuid.uuid4()),
    #                                                 body=json.dumps(data_object))
    #                         else:
    #                             es_client.create(index=index_name, doc_type='json', id=str(uuid.uuid4()),
    #                                          body=json.dumps(data_object))
    #                     except TransportError as e:
    #                         raise ValueError("Problem in {} connection, Error is {}".format("localhost", e.message))
    #                 else:
    #                     df.writelines(json.dumps(data_object) + '\n')
    # df.close()


if __name__ == '__main__':
    reddit = praw.Reddit(client_id="GpPKbxxnRcusfrS9ssV3PQ",
                         client_secret="DhwynfriKrYAP1jZ8BIc3C7NsR_dfA",
                         user_agent="my user agent",
                         username="ashimo95",
                         password="GoGetOne123$")
    topic_list = []
    es_client = Elasticsearch(host="search-cs242-knionzyqzacbeqerlruchgu6rm.us-west-2.es.amazonaws.com",
                              http_auth=('root', 'GoGetOne123$'), port=443, use_ssl=True)

    with open("subreedit_list.txt") as f:
        for topic in f.readlines():
            topic_list.append(topic.rstrip())
    subject = ['machine', 'learning', 'nlp', 'deep learning', 'deep', 'neural', 'network', 'active',
    'regression','linear', 'image', 'word', 'vector', 'maths', 'calculus', 'derivative', 'integration','natural', 'process',
               'loss','function', 'lambda', 'adam', 'optimizer', 'error', 'accuracy']
    info('main line')
    for t in topic_list:
        print("crawling for topic: {}".format(t))
        p = Process(target=SingleProcessRedditCrawler, args=(reddit, t, subject, es_client,))
        p.start()
