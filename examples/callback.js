import impala from '../';

const client = impala.createClient({
  resultType: 'json-array'
});

const sql = 'SELECT * FROM sample_07 LIMIT 5';

client.query(sql, (err, result) => {
  if (err) {
    console.error(err);
  } else {
    console.log(result);
  }
});
