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

