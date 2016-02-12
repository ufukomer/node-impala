"use strict";

var thrift = require('thrift');
var assert = require('assert');
var service = require('../lib/thrift/ImpalaService');
var types = require('../lib/thrift/beeswax_types');
var config = require('./config');

var connection = thrift.createConnection(config.server, config.port, {
    transport: thrift.TBufferedTransport,
    protocol: thrift.TBinaryProtocol
});

connection.on('error', function (err) {
    assert.ifError(err);
});

var query = new types.Query({
    query: "SELECT * FROM " + config.table + " LIMIT 5"
});

var client = thrift.createClient(service, connection);

client.query(query, function (err, handle) {
    assert.ifError(err);
    client.get_state(handle, function (err, state) {
        assert.ifError(err);
        client.explain(query, function (err, explain) {
            assert.ifError(err);
            client.fetch(handle, false, 100, function (err, result) {
                assert.ifError(err);
                client.get_results_metadata(handle, function (err, metaData) {
                    assert.ifError(err);
                    assert.ok(explain.textual);
                    assert.equal(5, result.data.length);
                    assert.ok(metaData.schema.fieldSchemas[0].name);
                    assert.ok(metaData.schema.fieldSchemas[0].type);
                    if (state === types.QueryState.FINISHED) {
                        connection.end();
                    }
                });
            });
        });
    });
});
