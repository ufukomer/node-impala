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
    var deferred = Q.defer(),
        connection = thrift.createConnection(this.host, this.port, this.options),
        client = thrift.createClient(service, connection),
        query = new types.Query({ query: sql });

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
            var map = new Map();
            for (var j = 0; j < metaData.schema.fieldSchemas.length; j++) {
                var resultArray = [];
                for (var i = 0; i < result.data.length; i++) {
                    resultArray.push(result.data[i].split("\t")[j]);
                }
                map.set(metaData.schema.fieldSchemas[j].name, resultArray);
            }
            deferred.resolve(map);
            return state;
        })
        .then(function (state) {
            if (state === types.QueryState.FINISHED) {
                connection.end();
            }
        })
        .catch(function (err) {
            connection.end();
            deferred.reject(err);
        });

    deferred.promise.nodeify(callback);
    return deferred.promise;
};

module.exports.createClient = function (props) {
    return new ImpalaClient(props);
};
