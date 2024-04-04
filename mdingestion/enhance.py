import requests

graphql_query = """
{
  datasets(hasCitations: 1, query: "wdcc", first:135) {
    totalCount
    nodes {
      citationCount
      id
      titles {
      	title
      }
      repository { 
        name
      }
    }
  }
}
"""

graphql_payload = {
        "query": graphql_query}

graphql_url = "https://api.datacite.org/graphql"

headers = {
        "Content-Type": "application/json"
    }

def count_citations():
    response = requests.post(graphql_url, json=graphql_payload, headers=headers)
    if response.ok:
        result = response.json()
     #   print (result)
    else:
        print(response.text)
        result = None
    lookup_cit = {}
    if result:
        for record in result['data']['datasets']['nodes']:
            id = record['id']
            citationCount = record['citationCount']
            lookup_cit[id.lower()] = citationCount
    return lookup_cit
