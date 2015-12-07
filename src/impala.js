"use strict";

/**
 * @module ThriftImpala
 */
var thrift = require('thrift');

function impalaClient(config) {

    var connect = function (onError, connected) {
        var host = config.host;
        var port = config.port || 21000;
        var version = config.version || 'chd5-2.1_5.3.0';
        var transport = config.transport || thrift.TBufferedTransport;
        var protocol = config.protocol || thrift.TBinaryProtocol;
        var timeout = config.timeout || 1000;
        var options = {
            transport: transport,
            protocol: protocol,
            timeout: timeout
        };

        var beeswax_types = require('./' + version + '/beeswax_types');
        var connection = thrift.createConnection(host, port, options);
        var client = thrift.createClient(require('./' + version + '/ImpalaService'), connection);

        var propagate = function (func, args) {
            var arr = [];
            for (var i = 2; i < args.length; i++) {
                arr.push(args[i]);
            }
            func.apply(null, arr);
        };

        var continueOnSuccess = function (err, onSuccess) {
            if (err) {
                connection.end();
                console.error(err);
            } else {
                propagate(onSuccess, arguments)
            }
        };

        connected({
            query: function (query, onSuccess) {
                client.query(query, function (err, handle) {
                    continueOnSuccess(err, onSuccess, handle);
                });
            },
            get_state: function (handle, onSuccess) {
                client.get_state(handle, function (err, state) {
                    continueOnSuccess(err, onSuccess, state);
                });
            },
            explain: function (query, onSuccess) {
                client.explain(query, function (err, explain) {
                    continueOnSuccess(err, onSuccess, explain);
                });
            },
            fetch: function (handle, onSuccess) {
                client.fetch(handle, false, 100, function (err, result) {
                    continueOnSuccess(err, onSuccess, result);
                });
            },
            get_results_metadata: function (handle, onSuccess) {
                client.get_results_metadata(handle, function (err, metaData) {
                    continueOnSuccess(err, onSuccess, metaData);
                });
            },
            closeConnection: function (state) {
                if (state === beeswax_types.QueryState.FINISHED)
                    connection.end();
            }
        });
    };

    return {
        query: function (query, onCompletion) {
            connect(onCompletion, function (client) {
                client.query(query, function (handle) {
                    client.get_state(handle, function (state) {
                        client.explain(query, function (explain) {
                            client.fetch(handle, function (result) {
                                client.get_results_metadata(handle, function (metaData) {
                                    client.closeConnection(state);
                                    console.log(explain.textual);
                                    console.log(metaData.schema.fieldSchemas);
                                    console.log(result.data);
                                });
                            });
                        });
                    });
                });
            });
        }
    }
}


module.exports.createClient = function (config) {
    return new impalaClient(config);
};