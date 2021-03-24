const { Client } = require('@elastic/elasticsearch');

const ELASTIC_SEARCH_URI = 'http://localhost:9200';
const INDEX_NAME = '911-calls';

const client = new Client({ node: ELASTIC_SEARCH_URI});

function Q1(then) {
  console.log("######## QUESTION 1 ########");

  // Alternative : on pourrait aussi simplement extraire la catégorie au moment de l'écriture des données
  client.search({index: INDEX_NAME, body: {
    "size": 0,
    "aggs" : {
      "categories" : {
        "filters": {
          "filters": {
            "EMS": {"prefix": {"title" : "EMS:"}},
            "Fire": {"prefix": {"title" : "Fire:"}},
            "Traffic": {"prefix": {"title" : "Traffic:"}}
          },
        },
      }
    }
  }}, (err, resp) => {
    console.log(resp.body.aggregations.categories.buckets);
    if (then) then();
  });
}


function Q2(then) {
  console.log("######## QUESTION 2 ########");

  client.search({index: INDEX_NAME, body: {
    "size": 0,
    "aggs": {
      "months": {
        "date_histogram": {
          "field": "timeStamp",
          "calendar_interval": "month",
          "order": {"_count": "desc"}
        },
        "aggs": {
          "top2": {
              "bucket_sort": {
                  "sort": {"_count": "desc"},
                  "size": 3
              }
          }
        }
      }
    }
  }}, (err, resp) => {
    // Formatting
    console.log(resp.body.aggregations.months.buckets.map(it => {
      const d = new Date(it.key_as_string);
      return {
        month: `${d.getFullYear()}-${("0" + (d.getMonth() + 1)).slice(-2)}`,
        doc_count: it.doc_count,
      };
    }))
    if (then) then();
  });
}


function Q3(then) {
  console.log("######## QUESTION 3 ########");

  client.search({index: INDEX_NAME, body: {
    "query": {
      "wildcard": {
          "title": {
            "value": "*OVERDOSE"
          }
      }
    },
    "size": 0,
    "aggs": {
      "locations" : {
        "terms" : {
            "field": "twp.keyword",
            "order" : { "_count" : "desc" },
            "size": 3
        }
      }
    }
  }}, (err, resp) => {
    console.log(resp.body.aggregations.locations.buckets)
    if (then) then();
  });
}


function Q4(then) {
  console.log("######## QUESTION 4 ########");

  client.count({index: INDEX_NAME, body: {
    "query": {
      "bool": {
        "must": {
          "match_all": {}
        },
        "filter": {
          "geo_distance": {
            "distance": "500m",
            "location": {
              "lat": 40.241493,
              "lon": -75.283783
            }
          }
        }
      }
    }
  }}, (err, resp) => {
    console.log(resp.body.count)
    if (then) then();
  });
}


Q1(() => Q2(() => Q3(() => Q4())));
