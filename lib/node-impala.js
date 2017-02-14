'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.createClient = undefined;

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }(); /**
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      * @module NodeImpala
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      */

/* eslint-disable import/extensions, import/no-unresolved */


var _thrift = require('thrift');

var _thrift2 = _interopRequireDefault(_thrift);

var _beeswax_types = require('./thrift/beeswax_types');

var _beeswax_types2 = _interopRequireDefault(_beeswax_types);

var _ImpalaService = require('./thrift/ImpalaService');

var _ImpalaService2 = _interopRequireDefault(_ImpalaService);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

/* eslint-enable */

/**
 * The class contains essential functions for executing queries
 * via Beeswax Service.
 */
var ImpalaClient = function () {
  function ImpalaClient() {
    _classCallCheck(this, ImpalaClient);
  }

  _createClass(ImpalaClient, [{
    key: 'connect',


    /**
     * Creates connection using given props.
     *
     * @param props {object}
     * @param callback {function}
     * @returns {function|promise}
     */
    value: function connect() {
      var props = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
      var callback = arguments[1];

      this.host = props.host || '127.0.0.1';
      this.port = props.port || 21000;
      this.resultType = props.resultType || null;
      this.timeout = props.timeout || 1000;
      this.transport = props.transport || _thrift2.default.TBufferedTransport;
      this.protocol = props.protocol || _thrift2.default.TBinaryProtocol;
      this.options = {
        transport: this.transport,
        protocol: this.protocol,
        timeout: this.timeout
      };

      var deferred = _thrift.Q.defer();

      var connection = _thrift2.default.createConnection(this.host, this.port, this.options);
      var client = _thrift2.default.createClient(_ImpalaService2.default, connection);

      connection.on('error', ImpalaClient.onErrorDeferred(deferred));

      connection.on('connect', function () {
        deferred.resolve('Connection is established.');
      });

      this.client = client;
      this.connection = connection;

      deferred.promise.nodeify(callback);
      return deferred.promise;
    }

    /**
     * Closes the current connection.
     *
     * @param callback {function}
     * @returns {function|promise}
     */

  }, {
    key: 'close',
    value: function close(callback) {
      var deferred = _thrift.Q.defer();
      var conn = this.connection;

      if (!conn) {
        deferred.reject(new Error('Connection was not created.'));
      } else {
        conn.end();
        deferred.resolve('Connection has ended.');
      }

      deferred.promise.nodeify(callback);
      return deferred.promise;
    }

    /**
     * Gets the query plan for a query.
     *
     * @param sql {string}
     * @param callback {function}
     * @returns {function|promise}
     */

  }, {
    key: 'explain',
    value: function explain(sql, callback) {
      var deferred = _thrift.Q.defer();
      var query = ImpalaClient.createQuery(sql);
      var client = this.client;

      if (!client) {
        deferred.reject(new Error('Connection was not created.'));
      } else {
        client.explain(query).then(function (explanation) {
          deferred.resolve(explanation);
        }).catch(function (err) {
          return deferred.reject(err);
        });
      }

      deferred.promise.nodeify(callback);
      return deferred.promise;
    }

    /**
     * Gets the result metadata.
     *
     * @param sql {string}
     * @param callback {function}
     * @returns {function|promise}
     */

  }, {
    key: 'getResultsMetadata',
    value: function getResultsMetadata(sql, callback) {
      var deferred = _thrift.Q.defer();
      var query = ImpalaClient.createQuery(sql);
      var client = this.client;

      if (!client) {
        deferred.reject(new Error('Connection was not created.'));
      } else {
        client.query(query).then(function (handle) {
          return [handle, client.get_results_metadata(handle)];
        }).spread(function (handle, metaData) {
          deferred.resolve(metaData);

          return handle;
        }).then(function (handle) {
          client.clean(handle.id);
          client.close(handle);
        }).catch(function (err) {
          return deferred.reject(err);
        });
      }

      deferred.promise.nodeify(callback);
      return deferred.promise;
    }

    /**
     * Transmits SQL command and receives result via Beeswax Service.
     * The query runs asynchronously.
     *
     * @param sql {string}
     * @param callback {function}
     * @returns {function|promise}
     */

  }, {
    key: 'query',
    value: function query(sql, callback) {
      var deferred = _thrift.Q.defer();
      var query = ImpalaClient.createQuery(sql);
      var resultType = this.resultType;
      var client = this.client;
      var connection = this.connection;

      if (!client || !connection) {
        deferred.reject(new Error('Connection was not created.'));
      } else {
        // increase the maximum number of listeners by 1
        // while this promise is in progress
        connection.setMaxListeners(connection.getMaxListeners() + 1);

        var onErrorCallback = ImpalaClient.onErrorDeferred(deferred);
        connection.on('error', onErrorCallback);

        client.query(query).then(function (handle) {
          return [handle, client.get_state(handle)];
        }).spread(function (handle, state) {
          return [handle, state, client.fetch(handle)];
        }).spread(function (handle, state, result) {
          return [handle, state, result, client.get_results_metadata(handle)];
        }).spread(function (handle, state, result, metaData) {
          var schemas = metaData.schema.fieldSchemas;
          var data = result.data;
          var processedData = ImpalaClient.processData(data, schemas, resultType);

          deferred.resolve(processedData);

          return handle;
        }).then(function (handle) {
          client.clean(handle.id);

          // this promise is done, so we lower the maximum number of listeners
          connection.setMaxListeners(connection.getMaxListeners() - 1);
          connection.removeListener('error', onErrorCallback);

          client.close(handle);
        }).catch(function (err) {
          return deferred.reject(err);
        });
      }

      deferred.promise.nodeify(callback);
      return deferred.promise;
    }
  }]);

  return ImpalaClient;
}();

/**
 * Creates Beeswax type query object.
 *
 * @param sql {string} SQL statement as string or Beeswax query object.
 * @returns {object}
 */


ImpalaClient.createQuery = function (sql) {
  if (sql instanceof _beeswax_types2.default.Query) {
    return sql;
  }

  return new _beeswax_types2.default.Query({ query: sql });
};

/**
 * Processes then returns the data according to desired type.
 *
 * @param data {object}
 * @param schemas {object}
 * @param type {string}
 * @returns {object} processed as the given data.
 */
ImpalaClient.processData = function (data, schemas, type) {
  switch (type) {
    case 'map':
      {
        var resultArray = [];
        var map = new Map();

        for (var i = 0; i < schemas.length; i += 1) {
          resultArray = [];
          for (var j = 0; j < data.length; j += 1) {
            resultArray.push(data[j].split('\t')[i]);
          }
          map.set(schemas[i].name, resultArray);
        }

        return map;
      }
    case 'json-array':
      {
        var resultObject = {};
        var array = [];
        var schemaNames = [];

        for (var _i = 0; _i < schemas.length; _i += 1) {
          schemaNames.push(schemas[_i].name);
        }

        for (var _i2 = 0; _i2 < data.length; _i2 += 1) {
          resultObject = {};
          for (var _j = 0; _j < schemaNames.length; _j += 1) {
            resultObject[schemaNames[_j]] = data[_i2].split('\t')[_j];
          }
          array.push(resultObject);
        }

        return array;
      }
    case 'boolean':
      {
        return data !== undefined && schemas !== undefined;
      }
    default:
      {
        return [data, schemas];
      }
  }
};

ImpalaClient.onErrorDeferred = function (deferred) {
  return function (err) {
    deferred.reject(err);
  };
};

// eslint-disable-next-line import/prefer-default-export
var createClient = exports.createClient = function createClient(props) {
  return new ImpalaClient(props);
};