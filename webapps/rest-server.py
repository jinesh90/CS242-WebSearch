from elasticsearch import Elasticsearch
from flask import Flask, request, render_template


ES_ENDPOINT = ''


es_client = Elasticsearch(host="",
                          http_auth=('', ''), port=443, use_ssl=True,timeout=60, max_retries=10,
                          retry_on_timeout=True)


class ElasticClient:
    """
    es handler
    """
    def __init__(self):
        try:
            self.es_client = Elasticsearch(host=ES_ENDPOINT,  http_auth=('root', 'GoGetOne123$'), port=443, use_ssl=True,timeout=60, max_retries=10, retry_on_timeout=True)
        except:
            print ("Could not connect with es: {}".format(ES_ENDPOINT))

    def search(self, query, top=10):
        # ES query
        query = {
            "query": {
                "query_string": {
                    "query": str(query)
                }
            }
        }
        print (query)
        search_result = self.es_client.search(
            index="*",
            body=query,
            sort=['_score:desc'],
            size=top
        )
        print("Total :{}".format(search_result.get('hits').get('total')))
        content_list = search_result.get('hits').get('hits')
        if len(content_list) == 0:
            status = "No test results were found for query: {}".format(query)
            return {}
        else:
            return content_list

app = Flask("Reddit Search Engine Management")


@app.route('/', methods=['GET', 'POST'])
def index():
    es = ElasticClient()
    if request.method == 'POST':

        if "search_term" not in request.form.keys():
            return render_template('form.html', error='Please select a search type')

        if "search_type"  not in request.form.keys():
            return render_template('form.html', error='Please Enter Search term')


        # get search term from form input
        search_term = request.form['search_term']

        # get selected search type from form input
        search_type = request.form['search_type']

        # make a request to the appropriate API endpoint with the search term as a parameter
        if search_type == 'lucene':
            response = es.search(search_term)
        elif search_type == 'bert':
            response = es.search(search_term)
        else:
            return 'Invalid search type'

        print(response)

        # render the search results template with the results
        return render_template('result.html', results=response)

    # render the search form template if no search term has been submitted yet
    return render_template('form.html')


if __name__ == '__main__':
    app.run(debug=True)
