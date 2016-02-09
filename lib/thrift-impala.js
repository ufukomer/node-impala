"use strict";

/**
 * @module ThriftImpala
 */
var thrift = require('thrift');
var Q = require('q');
var types = require('./thrift/beeswax_types');
var service = require('./thrift/ImpalaService');

module.exports.createClient = function (props) {
    return new ImpalaClient(props);
};

/**
 * Constructor method of ImpalaClient which initializes
 * essential properties for connection.
 *
 * @param props
 * @constructor
 */
function ImpalaClient(props) {
    this.host = props.host;
    this.port = props.port || 21000;
    this.transport = props.transport || thrift.TBufferedTransport;
    this.protocol = props.protocol || thrift.TBinaryProtocol;
    this.options = {
        transport: this.transport,
        protocol: this.protocol
    };
}

/**
 * Transmits SQL commands and receive results by Beeswax.
 *
 * @param sql
 * @param callback
 * @returns {*|promise}
 */
ImpalaClient.prototype.query = function (sql, callback) {
    var deferred = Q.defer();
    var connection = thrift.createConnection(this.host, this.port, this.options);
    var client = thrift.createClient(service, connection);
    var query = new types.Query({query: sql});

    client.query(query, function (err, handle) {
        if (err) console.error(err);
        else {
            client.get_state(handle, function (err, state) {
                if (err) console.error(err);
                else {
                    client.explain(query, function (err, explain) {
                        if (err) console.error(err);
                        else {
                            client.fetch(handle, false, 100, function (err, result) {
                                if (err) console.error(err);
                                else {
                                    client.get_results_metadata(handle, function (err, metaData) {
                                        if (err) console.error(err);
                                        else {
                                            deferred.resolve(result.data);
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });
        }
    });

    deferred.promise.nodeify(callback);
    return deferred.promise;
};
