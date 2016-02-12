# node-impala

> Node client for Impala.

## Install

```sh
$ npm install --save node-impala
```

## Usage

```js
var impala = require('node-impala');

var client = impala.createClient();

client.query("SELECT column_name FROM table_name", function (err, result) {
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
var impala = require('node-impala');

var client = impala.createClient({
    host: '192.168.93.128',
    mapping: false
});

var sql = "SELECT * FROM sample_07 LIMIT 5";

client.query(sql, function (err, result) {
    if (err) {
        console.error(err);
    } else {
        console.log(result);
    }
});
```

### Using promise

```js
var impala = require('node-impala');

var client = impala.createClient({
    host: '192.168.93.128',
    timeout: 5000
});

var sql = "SELECT * FROM sample_07 limit 5";

client.query(sql)
    .then(function (result) {
        console.log(result);
    })
    .catch(function (err) {
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
You should leave it as its default if you are connecting from
virtual machine.

### port

Type: `number`<br>
Default: `21000`

Default value corresponds to Impala Daemon Frontend Port which
is used to transmit commands and receive results by Beeswax.

### mapping

Type: `boolean`<br>
Default: `true`

Maps query results with schema names.

### timeout

Type: `number`<br>
Default: `1000`

Timeout value for closing connection if it remains open.

## Contributing

Pull requests must follow coding standard. Please scrutinize
commits carefully.

## License

MIT © [ufukomer](http://ufukomer.com)
