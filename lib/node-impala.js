'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }(); /**
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      * @module NodeImpala
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      */


var _thrift = require('thrift');

var _thrift2 = _interopRequireDefault(_thrift);

var _beeswax_types = require('./thrift/beeswax_types');

var _beeswax_types2 = _interopRequireDefault(_beeswax_types);

var _ImpalaService = require('./thrift/ImpalaService');

var _ImpalaService2 = _interopRequireDefault(_ImpalaService);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

/**
 * The class contains essential functions for executing
 * queries via Beeswax service.
 *
 * @param props
 * @class
 */

var ImpalaClient = function () {
  function ImpalaClient() {
    var props = arguments.length <= 0 || arguments[0] === undefined ? {} : arguments[0];

    _classCallCheck(this, ImpalaClient);

    this.host = props.host || '127.0.0.1';
    this.port = props.port || 21000;
    this.resultType = props.resultType || null;
    this.timeout = props.timeout || 1000;
    this.transport = props.transport || _thrift2.default.TBufferedTransport;
    this.protocol = props.protocol || _thrift2.default.TBinaryProtocol;
    this.options = {
      transport: this.transport,
      protocol: this.protocol
    };
  }

  /**
   * Transmits SQL commands and receives results by Beeswax.
   *
   * @param sql
   * @param callback
   * @returns {*|promise}
   */


  _createClass(ImpalaClient, [{
    key: 'query',
    value: function query(sql, callback) {
      var deferred = _thrift.Q.defer();
      var connection = _thrift2.default.createConnection(this.host, this.port, this.options);
      var client = _thrift2.default.createClient(_ImpalaService2.default, connection);
      var query = ImpalaClient.createQuery(sql);
      var resultType = this.resultType;
      var timeout = this.timeout;

      client.query(query).then(function (handle) {
        return [handle, client.get_state(handle)];
      }).spread(function (handle, state) {
        return [handle, state, client.fetch(handle)];
      }).spread(function (handle, state, result) {
        return [state, result, client.get_results_metadata(handle)];
      }).spread(function (state, result, metaData) {
        var schemas = metaData.schema.fieldSchemas;
        var data = result.data;
        var processedData = ImpalaClient.processData(data, schemas, resultType);

        deferred.resolve(processedData);

        return state;
      }).then(function (state) {
        if (state === _beeswax_types2.default.QueryState.FINISHED) {
          connection.end();
        }
      }).catch(function (err) {
        return deferred.reject(err);
      }).finally(function () {
        if (connection.connected) {
          setTimeout(function () {
            connection.end();
          }, timeout);
        }
      });

      deferred.promise.nodeify(callback);
      return deferred.promise;
    }
  }]);

  return ImpalaClient;
}();

/**
 * Creates Beeswax query object.
 *
 * @param sql SQL statement as string or Beeswax query object
 * @returns {*}
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
 * @param data
 * @param schemas
 * @param type
 * @returns {object}
 */
ImpalaClient.processData = function (data, schemas, type) {
  switch (type) {
    case 'map':
      {
        var resultArray = [];
        var map = new Map();

        for (var i = 0; i < schemas.length; i++) {
          resultArray = [];
          for (var j = 0; j < data.length; j++) {
            resultArray.push(data[j].split('\t')[i]);
          }
          map.set(schemas[i].name, resultArray);
        }

        return map;
      }
    case 'array':
      {
        var resultObject = {};
        var array = [];
        var schemaNames = [];

        for (var _i = 0; _i < schemas.length; _i++) {
          schemaNames.push(schemas[_i].name);
        }

        for (var _i2 = 0; _i2 < data.length; _i2++) {
          resultObject = {};
          for (var _j = 0; _j < schemaNames.length; _j++) {
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

module.exports.createClient = function (props) {
  return new ImpalaClient(props);
};
