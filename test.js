/* eslint-disable no-shadow, no-unused-vars, no-console */
const RedisServer = require('redis-server');
const async = require('async');
const lodash = require('lodash');

const RedisShard = require('./index');

const PORT = 7000;
const NUM = 5;
const TESTS = {};

TESTS.keysCommandTests = ({ KEY_NUM, redis }, callback) => {
  const keys = lodash.times(KEY_NUM, n => `fooregexx${n}`);
  const values = lodash.times(KEY_NUM, n => `barregexx${n}`);

  const keys2 = lodash.times(KEY_NUM, n => `fooregex${n}`);
  const values2 = lodash.times(KEY_NUM, n => `barregex${n}`);

  const allKeys = lodash.concat(keys, keys2);
  const allValues = lodash.concat(values, values2);

  const msetArgs = lodash
    .chain(allKeys)
    .zip(allValues)
    .flatten()
    .value();

  const regex1 = 'fooregexx*'; // meant to match keys
  const regex2 = 'fooregex*'; // meant to match allKeys

  async.auto({
    mset: callback => redis.mset(msetArgs, callback),
    keys1: ['mset', callback => redis.keys(regex1, callback)],
    keys2: ['mset', callback => redis.keys(regex2, callback)]
  }, (err, { mset, keys1, keys2 }) => {
    if (err) {
      return callback(err);
    }
    if (mset !== 'OK') {
      return callback('mset was not successful');
    }

    const sortedKeys1 = lodash.sortBy(keys);
    const sortedKeys2 = lodash.sortBy(allKeys);

    const retrievedKeys1 = lodash.sortBy(keys1);
    const retrievedKeys2 = lodash.sortBy(keys2);

    if (
      !lodash.isEqual(sortedKeys1, retrievedKeys1)
      || !lodash.isEqual(sortedKeys2, retrievedKeys2)
    ) {
      return callback('Keys response doesn\'t match values');
    }
    return callback();
  });
};

TESTS.multiSetGetCommandTests = ({ KEY_NUM, redis }, callback) => {
  const keys = lodash.times(KEY_NUM, n => `foo${n}`);
  const values = lodash.times(KEY_NUM, n => `bar${n}`);

  const msetArgs = lodash
    .chain(keys)
    .zip(values)
    .flatten()
    .value();

  async.auto({
    mset: callback => redis.mset(msetArgs, callback),
    mget: ['mset', callback => redis.mget(keys, callback)]
  }, (err, { mset, mget }) => {
    if (err) {
      return callback(err);
    }
    if (mset !== 'OK') {
      return callback('mset was not successful');
    }
    if (!lodash.isEqual(mget, values)) {
      return callback('Mget response doesn\'t match values');
    }
    return callback();
  });
};

async.times(NUM, (index, next) => {
  const server = new RedisServer(PORT + index);
  server.open(next);
}, (err) => {
  const options = {
    servers: lodash.map(lodash.times(NUM, n => `127.0.0.1:${PORT + n}`))
  };
  const redis = new RedisShard(options);
  const KEY_NUM = 1000;

  const args = { KEY_NUM, redis };

  const asyncArgs = lodash.reduce(
    TESTS,
    (acc, value, key) => {
      acc[key] = lodash.partial(value, args);
      return acc;
    },
    {}
  );

  async.auto(asyncArgs, (err) => {
    if (err) {
      throw err;
    }
    console.log('Success. All tests passed.');
    process.exit(0);
  });
});
