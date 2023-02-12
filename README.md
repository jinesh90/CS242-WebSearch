# CS242-WebSearch

## How to run this project?

for running Elastic Search locally 

```bash
docker pull elasticsearch:7.4.0
docker run -d --name es740 -p 9200:9200 -e "discovery.type=single-node" elasticsearch:7.4.0
```

for running Kibana Locally
```bash
docker pull kibana:7.4.0
docker run -d --name kibana --link es740:elasticsearch -p 5601:5601 kibana:7.4.0

```

Once you have running docker containers successfully, you can verify by opening "http://localhostL5601", 
you will see Kibana web interface.

After, running Elastic search locally, run crawler to crawl data and insert it.

# How to run crawler?
Crawler is using multiprocessing for crawling data from different-different subreddit posts.( e.g r/MachineLearning), these subreddit written in subreedit_list.txt file, you can expand this file. Once you have listed all subreddit topic. you can provide your developer credentials ( client_id,client_secret,username,password), By default crawler saves files locally in json line format.If you want store data on elasticsearch. you can set `local_save` parameter to False.to run crawler, follow the steps below
 -Step 1: Install necessary packages from requirements.txt files
 -Step 2: Make sure you have enough space ( ~800 MB).
 -Step 3: run run.sh file, like `/.run.sh` 
 -Step 4: will see crwaler in action.
