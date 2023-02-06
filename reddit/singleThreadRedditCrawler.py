import praw
import json
import uuid
from datetime import datetime
from elasticsearch import Elasticsearch, TransportError


def SingleThreadRedditCrawler(reddit, topic, subject):
    """
    single thread reddit crawler based on subreddit.
    :param reddit: reddit connection object
    :param topic: subreddit topics like "r/MachineLearning, r/DataScience"
    :param subject: search query for sub reddit.
    :return:
    """
    for sub in topic:
        subreddit = reddit.subreddit(sub)
        query = [subject]
        for item in query:
            data_object = dict()
            for submission in subreddit.search(query, sort="new", limit=1000):
                data_object["reddit_subreddit"] = sub
                data_object["reddit_subreddit_query"] = item
                data_object["title"] = submission.title
                data_object["reddit_id"] = submission.id
                data_object["total_comments"] = submission.num_comments
                #print(datetime.fromtimestamp(submission.created))
                data_object["created"] = submission.created
                data_object["text"] = submission.selftext
                data_object["url"] = submission.url
                dt_object = datetime.fromtimestamp(submission.created)
                index_name = "{}-{}".format(sub.lower(), dt_object.strftime('%Y-%m-%d'))
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
                    es_client = Elasticsearch(host="localhost", port=9200)
                    if es_client.indices.exists(index=index_name):
                        es_client.index(index=index_name, doc_type='json', id=str(uuid.uuid4()),
                                        body=json.dumps(data_object))
                    else:
                        es_client.create(index=index_name, doc_type='json', id=str(uuid.uuid4()),
                                         body=json.dumps(data_object))
                except TransportError as e:
                    raise ValueError("Problem in {} connection, Error is {}".format("localhost", e.message))


if __name__ == '__main__':
    reddit = praw.Reddit(client_id="",
                         client_secret="",
                         user_agent="",
                         username="",
                         password="")
    topic = ["MachineLearning"]
    subject = "Machine Learning"
    SingleThreadRedditCrawler(reddit, topic, subject)
