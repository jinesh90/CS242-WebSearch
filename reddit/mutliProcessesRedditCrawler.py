import praw
import json
import uuid
from multiprocessing import Process
from datetime import datetime
from elasticsearch import Elasticsearch, TransportError


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
    with open("{}.txt".format(topic), "w+") as df:
        # create subreedit (r/MachineLearning, r/DataScience ) etc
        subreddit = reddit.subreddit(topic)
        for q in subject:
            query = [q]
            for item in query:
                data_object = dict()

                # user search api from reddit, note here search api is using cache and fater in response, so we can save I/O time
                for submission in subreddit.search(query, sort="new", limit=1000):
                    # Search api is giving generator object, create data object from result documents
                    data_object["reddit_subreddit"] = topic
                    data_object["reddit_subreddit_query"] = item

                    # title should be reddit post title
                    data_object["title"] = submission.title
                    data_object["reddit_id"] = submission.id

                    # number of comments in the post
                    data_object["total_comments"] = submission.num_comments

                    # time of the post created in epoch timestamp
                    data_object["created"] = (submission.created) * 1000

                    # text for
                    data_object["text"] = submission.selftext
                    data_object["url"] = submission.url
                    dt_object = datetime.fromtimestamp(submission.created)

                    #index name is based on subreddit topic.
                    index_name = str(topic).lower()
                    submission.comments.replace_more(limit=1)
                    comment_lists = list()
                    # iterate comments for buliding documets
                    for comment in submission.comments.list():
                        comment_object = dict()
                        comment_object["comment_id"] = comment.id
                        comment_object["comment_parent_id"] = comment.parent_id
                        comment_object["comment_body"] = comment.body
                        comment_object["comment_link_id"] = comment.link_id
                        comment_lists.append(comment_object)
                    data_object["comments"] = comment_lists

                    # option for local save or remote save to elasticsearch.
                    if not local_save:
                        try:
                            if es_client.indices.exists(index=index_name):
                                es_client.index(index=index_name, doc_type='json', id=str(uuid.uuid4()),
                                                body=json.dumps(data_object), timeout="6000s")
                            else:
                                es_client.create(index=index_name, doc_type='json', id=str(uuid.uuid4()),
                                             body=json.dumps(data_object))
                        except TransportError as e:
                            raise ValueError("Problem in {} connection, Error is {}".format(es_client, e.message))
                    else:
                        df.writelines(json.dumps(data_object) + '\n')
    df.close()


if __name__ == '__main__':

    # create reddit praw object with client id for creating developer access follow this link: https://medium.com/geekculture/utilizing-reddits-api-8d9f6933e192
    reddit = praw.Reddit(client_id="",
                         client_secret="",
                         user_agent="",
                         username="",
                         password="")
    topic_list = []

    # elastic search obj
    es_client = Elasticsearch(host="localhost", port=9200)

    # crawler target list
    with open("subreedit_list.txt") as f:
        for topic in f.readlines():
            topic_list.append(topic.rstrip())
    subject = ['machine', 'learning', 'nlp', 'deep learning', 'deep', 'neural', 'network', 'active',
    'regression','linear', 'image', 'word', 'vector', 'maths', 'calculus', 'derivative', 'integration','natural', 'process',
               'loss','function', 'lambda', 'adam', 'optimizer', 'error', 'accuracy']

    # create multiple processes per subreddit topics mentioned in subreddit_list.txt
    for t in topic_list:
        print("crawling for topic: {}".format(t))
        p = Process(target=SingleProcessRedditCrawler, args=(reddit, t, subject, es_client,))
        p.start()
