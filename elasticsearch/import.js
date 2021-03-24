const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

const ELASTIC_SEARCH_URI = 'http://localhost:9200';
const INDEX_NAME = '911-calls';

async function run() {
  const client = new Client({ node: ELASTIC_SEARCH_URI});

  // Drop index if exists
  await client.indices.delete({
    index: INDEX_NAME,
    ignore_unavailable: true
  });

  await client.indices.create({
    index: INDEX_NAME,
    body : {
      mappings: {
        properties: {
          location : {"type" : "geo_point"},
          title: {"type": "keyword"},
        }
      }
    }
  });

  const entries = [];

  fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
      const date = data.timeStamp.split(" ")[0];
      const dateSplit = date.split("-");
      const year = parseInt(dateSplit[0]);
      const month = parseInt(dateSplit[1]);

      const call = {
        location: {
          lat: parseFloat(data.lat),
          lon: parseFloat(data.lng),
        },
        desc: data.desc,
        zip: data.zip,
        title: data.title,
        timeStamp: date, // send only YYYY-MM-dd to avoid one-off issues in months
        bimestre: year + "-" + Math.floor(month / 2),
        twp: data.twp,
        addr: data.addr,
      };
      entries.push(call);
    })
    .on('end', async () => {
      // Run the insert query
      client.bulk(createBulkInsertQuery(entries), (err, resp) => {
        if (err) console.trace(err.message);
        else console.log(`Inserted ${resp.body.items.length} entries`);

        client.close();
      });
    });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(entries) {
  const body = entries.reduce((acc, entry) => {
    acc.push({
      index: {
        _index: INDEX_NAME,
        _type: '_doc',
      },
    });
    acc.push(entry)
    return acc
  }, []);

  return { body };
}

run().catch(console.log);


