/**
 * @module NodeImpala
 */
import thrift, { Q } from 'thrift';
import types from './thrift/beeswax_types';
import service from './thrift/ImpalaService';

/**
 * The class contains essential functions for executing queries
 * via Beeswax Service.
 */
class ImpalaClient {

  /**
   * Constructor assigns essential instance values to start connection.
   *
   * @param props {object}
   */
  constructor(props = {}) {
    this.host = props.host || '127.0.0.1';
    this.port = props.port || 21000;
    this.resultType = props.resultType || null;
    this.timeout = props.timeout || 1000;
    this.transport = props.transport || thrift.TBufferedTransport;
    this.protocol = props.protocol || thrift.TBinaryProtocol;
    this.options = {
      transport: this.transport,
      protocol: this.protocol
    };
  }

  /**
   * Transmits SQL command and receives result via Beeswax Service.
   *
   * @param sql {string}
   * @param callback {function}
   * @returns {function|promise}
   */
  query(sql, callback) {
    const deferred = Q.defer();
    const connection = thrift.createConnection(this.host, this.port, this.options);
    const client = thrift.createClient(service, connection);
    const query = ImpalaClient.createQuery(sql);
    const resultType = this.resultType;
    const timeout = this.timeout;

    client.query(query)
      .then(handle =>
        [handle, client.get_state(handle)]
      )
      .spread((handle, state) =>
        [handle, state, client.fetch(handle)]
      )
      .spread((handle, state, result) =>
        [state, result, client.get_results_metadata(handle)]
      )
      .spread((state, result, metaData) => {
        const schemas = metaData.schema.fieldSchemas;
        const data = result.data;
        const processedData = ImpalaClient.processData(data, schemas, resultType);

        deferred.resolve(processedData);

        return state;
      })
      .then((state) => {
        if (state > 2) {
          connection.end();
        }
      })
      .catch(err => deferred.reject(err))
      .finally(() => {
        if (connection.connected) {
          setTimeout(() => {
            connection.end();
          }, timeout);
        }
      });

    deferred.promise.nodeify(callback);
    return deferred.promise;
  }
}

/**
 * Creates Beeswax type query object.
 *
 * @param sql {string} SQL statement as string or Beeswax query object.
 * @returns {object}
 */
ImpalaClient.createQuery = (sql) => {
  if (sql instanceof types.Query) {
    return sql;
  }

  return new types.Query({ query: sql });
};

/**
 * Processes then returns the data according to desired type.
 *
 * @param data {object}
 * @param schemas {object}
 * @param type {string}
 * @returns {object} processed as the given data.
 */
ImpalaClient.processData = (data, schemas, type) => {
  switch (type) {
    case 'map': {
      let resultArray = [];
      const map = new Map();

      for (let i = 0; i < schemas.length; i++) {
        resultArray = [];
        for (let j = 0; j < data.length; j++) {
          resultArray.push(data[j].split('\t')[i]);
        }
        map.set(schemas[i].name, resultArray);
      }

      return map;
    }
    case 'json-array': {
      let resultObject = {};
      const array = [];
      const schemaNames = [];

      for (let i = 0; i < schemas.length; i++) {
        schemaNames.push(schemas[i].name);
      }

      for (let i = 0; i < data.length; i++) {
        resultObject = {};
        for (let j = 0; j < schemaNames.length; j++) {
          resultObject[schemaNames[j]] = data[i].split('\t')[j];
        }
        array.push(resultObject);
      }

      return array;
    }
    case 'boolean': {
      return (data !== undefined) && (schemas !== undefined);
    }
    default: {
      return [data, schemas];
    }
  }
};

module.exports.createClient = (props) => new ImpalaClient(props);
