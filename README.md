# node-impala [![Build Status](https://travis-ci.org/ufukomer/node-impala.svg?branch=master)](https://travis-ci.org/ufukomer/node-impala)

> Node client for Impala.

## Install

```sh
$ npm install --save node-impala
```

## Usage

```js
import impala from 'node-impala';

const client = impala.createClient();

client.query('SELECT column_name FROM table_name', (err, result) => {
  if (err) {
    console.error(err);
  } else {
    console.log(result);
  }
});
```

## Examples

### Using callback

```js
import impala from 'node-impala';

const client = impala.createClient({
  host: '192.168.93.128',
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
```

### Using promise

```js
import impala from 'node-impala';

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
Default: `boolean`

Returns result of query according to the given type.

Available variables:

- `json-array` returns json array
- `map` maps columns to rows
- `boolean` returns false if data is undefined

### timeout

Type: `number`<br>
Default: `1000`

Timeout value for closing connection if it remains open.

## Versions

`1.x.x` uses Impala `chd5-2.1_5.3.0`.

## License

MIT © [ufukomer](http://ufukomer.com)
