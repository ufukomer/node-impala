import test from 'ava';
import { createClient } from '../';

const client = createClient();

if (process.env.CI) {
  test('travis', (t) => {
    t.pass();
  });
} else {
  const sql = 'SELECT * FROM crimes LIMIT 5';

  test.before('create a new connection', () => {
    client.connect({
      host: '127.0.0.1',
      port: 21000,
      resultType: 'json-array'
    });
  });

  test('client should have props', (t) => {
    t.is(client.host, '127.0.0.1');
    t.is(client.port, 21000);
    t.is(client.resultType, 'json-array');
    t.is(client.timeout, 1000);
  });

  test('query should return the result', async (t) => {
    const result = await client.query(sql);
    t.is(5, result.length);
    t.truthy(result);
  });

  test('explain should return the query plan', async (t) => {
    const explanation = await client.explain(sql);
    t.truthy(explanation);
  });

  test('getResultsMetadata should return the result metadata', async (t) => {
    const metaData = await client.getResultsMetadata(sql);
    t.truthy(metaData);
  });

  test.after('close the connection', async (t) => {
    const error = await client.close();
    t.truthy(error);
  });
}
