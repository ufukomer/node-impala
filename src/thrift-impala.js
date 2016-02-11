"use strict";

/**
 * @module ThriftImpala
 */
var thrift = require('thrift');
var Q = require('q');
var types = require('./thrift/beeswax_types');
var service = require('./thrift/ImpalaService');

/**
 * Constructor method of ImpalaClient which initializes
 * essential properties for connection.
 *
 * @param props
 * @constructor
 */
function ImpalaClient(props) {
    this.props = typeof props !== 'undefined' ? Object.create(props) : {};
    this.host = typeof this.props.host !== 'undefined' ? this.props.host : '127.0.0.1';
    this.port = typeof this.props.port !== 'undefined' ? this.props.port : 21000;
    this.transport = typeof this.props.transport !== 'undefined' ? this.props.transport : thrift.TBufferedTransport;
    this.protocol = typeof this.props.protocol !== 'undefined' ? this.props.protocol : thrift.TBinaryProtocol;
    this.options = {
        transport: this.transport,
        protocol: this.protocol
    };
    this.mapping = typeof this.props.mapping !== 'undefined' ? this.props.mapping : true;
    this.timeout = typeof this.props.timeout !== 'undefined' ? this.props.timeout : 1000;
}

/**
 * Transmits SQL commands and receive results by Beeswax.
 *
 * @param sql
 * @param callback
 * @returns {*|promise}
 */
ImpalaClient.prototype.query = function (sql, callback) {
    var deferred = Q.defer(),
        connection = thrift.createConnection(this.host, this.port, this.options),
        client = thrift.createClient(service, connection),
        query = new types.Query({ query: sql }),
        mapping = this.mapping,
        timeout = this.timeout;

    client.query(query)
        .then(function (handle) {
            return [handle, client.get_state(handle)];
        })
        .spread(function (handle, state) {
            return [handle, state, client.fetch(handle)];
        })
        .spread(function (handle, state, result) {
            return [state, result, client.get_results_metadata(handle)];
        })
        .spread(function (state, result, metaData) {
            var schemas = metaData.schema.fieldSchemas,
                data = result.data,
                map = new Map();
            if (mapping) {
                for (var j = 0; j < schemas.length; j++) {
                    var resultArray = [];
                    for (var i = 0; i < data.length; i++) {
                        resultArray.push(data[i].split("\t")[j]);
                    }
                    map.set(schemas[j].name, resultArray);
                }
                deferred.resolve(map);
            } else {
                deferred.resolve([data, schemas]);
            }
            return state;
        })
        .then(function (state) {
            if (state === types.QueryState.FINISHED) {
                connection.end();
            }
        })
        .catch(function (err) {
            deferred.reject(err);
        })
        .finally(function () {
            if (connection.connected) {
                setTimeout(function () {
                    connection.end();
                }, timeout)
            }
        });

    deferred.promise.nodeify(callback);
    return deferred.promise;
};

module.exports.createClient = function (props) {
    return new ImpalaClient(props);
};
