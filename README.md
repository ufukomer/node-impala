# node-impala [![Build Status](https://travis-ci.org/ufukomer/node-impala.svg?branch=master)](https://travis-ci.org/ufukomer/node-impala)

> Node client for Impala.

## Install

```sh
$ npm install --save node-impala
```

## Usage

See the [issue](https://github.com/ufukomer/node-impala/issues/4) before using this module.

```js
import { createClient } from 'node-impala';

const client = createClient();

client.connect({
  host: '127.0.0.1',
  port: 21000,
  resultType: 'json-array'
});

client.query('SELECT column_name FROM table_name')
  .then(result => console.log(result))
  .catch(err => console.error(err))
  .done(() => client.close().catch(err => console.error(err)));
```

## Options

### host

Type: `string`<br>
Default: `'127.0.0.1'`

If you are connecting Thrift Server remotely, such as
connecting Cloudera from your host machine instead virtual
machine, it corresponds to inet address of virtual machine
that you can learn using `ifconfig` command in terminal.
Otherwise, you should leave it as its default if you are connecting from
virtual machine.

### port

Type: `number`<br>
Default: `21000`

Default value corresponds to Impala Daemon Frontend Port which
is used to transmit commands and receive results by Beeswax.

### resultType

Type: `string`<br>
Default: `null`

Returns result of query according to the given type.

Available variables:

- `json-array` returns json array
- `map` maps columns to rows
- `boolean` returns true if query is successful
- `null` returns results of query and table schemas as an array

### timeout

Type: `number`<br>
Default: `1000`

Timeout value for closing transport after process finished.

## API

### createClient()

Creates client that uses BeeswaxService.

```js
const client = createClient();
```

### client.connect(props, callback)

Creates connection using given props.

```js
client.connect({ resultType: 'boolean' })
  .then(message => console.log(message))
  .catch(error => console.error(error));
```

### client.close(callback)

Closes the current connection.

```js
client.close().catch((err) => console.error(err));
```

### client.explain(sql, callback)

Gets the query plan for a query.

```js
client.explain(sql)
  .then(explanation => console.log(explanation))
  .catch(err => console.error(err));
```

### client.getResultsMetadata(sql, callback)

Gets the result metadata.

```js
client.getResultsMetadata(sql)
  .then(metaData => console.log(metaData))
  .catch(err => console.error(err));
```

### client.query(sql, callback)

Transmits SQL command and receives result via Beeswax Service. The query runs asynchronously.

```js
client.query(sql)
  .then(results => console.log(results))
  .catch(err => console.error(err));
```

## Versions

`1.x.x` and `2.x.x` uses Impala `chd5-2.1_5.3.0`

## License

MIT © [ufukomer](http://ufukomer.com)
