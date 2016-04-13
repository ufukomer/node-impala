import * as impala from '../';

const client = impala.createClient({
  resultType: 'json-array'
});

const sql = 'SELECT * FROM sample_07 LIMIT 5';

client.query(sql)
  .then((result) => {
    console.log(result);
  })
  .catch((err) => {
    console.error(err);
  });
