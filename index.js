const assert = require('assert');
const HashRing = require('hashring');
const Redis = require('ioredis');
const step = require('step');
const lodash = require('lodash');
const async = require('async');

module.exports = function RedisShard(options) {
  assert(!!options, 'options must be an object');
  assert(Array.isArray(options.servers), 'servers must be an array');

  const self = {};
  const clients = {};
  options.servers.forEach((server) => {
    const clientOptions = options.clientOptions || {};
    const client = new Redis(server, clientOptions);
    if (options.database) {
      client.select(options.database, () => {});
    }
    if (options.password) {
      client.auth(options.password);
    }
    clients[server] = client;
  });

  const servers = {};
  Object.keys(clients).forEach((key) => {
    servers[key] = 1; // balanced ring for now
  });
  self.ring = new HashRing(servers);

  // All of these commands have 'key' as their first parameter
  const SHARDABLE = [
    'append', 'bitcount', 'blpop', 'brpop', 'debug object', 'decr', 'decrby', 'del', 'dump', 'exists', 'expire',
    'expireat', 'get', 'getbit', 'getrange', 'getset', 'hdel', 'hexists', 'hget', 'hgetall', 'hincrby',
    'hincrbyfloat', 'hkeys', 'hlen', 'hmget', 'hmset', 'hset', 'hsetnx', 'hvals', 'incr', 'incrby', 'incrbyfloat',
    'lindex', 'linsert', 'llen', 'lpop', 'lpush', 'lpushx', 'lrange', 'lrem', 'lset', 'ltrim', 'move',
    'persist', 'pexpire', 'pexpireat', 'psetex', 'pttl', 'rename', 'renamenx', 'restore', 'rpop', 'rpush', 'rpushx',
    'sadd', 'scard', 'sdiff', 'set', 'setbit', 'setex', 'setnx', 'setrange', 'sinter', 'sismember', 'smembers',
    'sort', 'spop', 'srandmember', 'srem', 'strlen', 'sunion', 'ttl', 'type', 'watch', 'zadd', 'zcard', 'zcount',
    'zincrby', 'zrange', 'zrangebyscore', 'zrank', 'zrem', 'zremrangebyrank', 'zremrangebyscore', 'zrevrange',
    'zrevrangebyscore', 'zrevrank', 'zscore'
  ];
  SHARDABLE.forEach((command) => {
    self[command] = (...args) => {
      const node = self.ring.get(args[0]);
      const client = clients[node];
      client[command](...args);
    };
  });

  // mget
  self.mget = async (...args) => {
    const keys = lodash.first(args);
    const callback = lodash.last(args);
    const mapping = lodash.reduce(keys, (acc, key) => {
      const node = self.ring.get(key);
      if (lodash.has(acc, node)) {
        acc[node].push(key);
      } else {
        acc[node] = [key];
      }
      return acc;
    }, {});
    const nodes = lodash.keys(mapping);
    async.map(nodes, (node, next) => {
      const nodeKeys = mapping[node];
      const client = clients[node];
      //      console.log(node, keys.length);
      client.mget(nodeKeys, next);
    }, (err, results) => {
      if (err) {
        return callback(err);
      }
      const keyHash = lodash.reduce(nodes, (acc, node, index) => {
        const nodeKeys = mapping[node];
        const values = lodash.get(results, [index], []);
        return lodash.assign(acc, lodash.zipObject(nodeKeys, values));
      }, {});
      return callback(err, lodash.map(keys, key => keyHash[key]));
    });
  };

  // keys
  self.keys = async (...args) => {
    const keyRegex = lodash.first(args);
    const callback = lodash.last(args);
    const nodes = lodash.keys(self.ring.vnodes);
    async.map(nodes, (node, next) => {
      const client = clients[node];
      client.keys(keyRegex, next);
    }, (err, results) => {
      if (err) {
        return callback(err);
      }
      let keys = [];
      // eslint-disable-next-line no-return-assign
      lodash.map(results, nodeKeys => keys = lodash.concat(keys, nodeKeys));
      return callback(err, keys);
    });
  };

  // mset
  self.mset = async (...args) => {
    const keys = lodash.first(args);
    const callback = lodash.last(args);
    const mapping = lodash.reduce(keys, (acc, key, index) => {
      const keyCheck = index % 2 ? keys[index - 1] : key;
      const node = self.ring.get(keyCheck);
      if (lodash.has(acc, node)) {
        acc[node].push(key);
      } else {
        acc[node] = [key];
      }
      return acc;
    }, {});
    const nodes = lodash.keys(mapping);
    async.map(nodes, (node, next) => {
      const nodeKeys = mapping[node];
      const client = clients[node];
      client.mset(nodeKeys, next);
      // eslint-disable-next-line no-unused-vars
    }, (err, responses) => {
      if (err) {
        return callback(err);
      }
      return callback(null, 'OK');
    });
  };

  // No key parameter to shard on - throw Error
  const UNSHARDABLE = [
    'auth', 'bgrewriteaof', 'bgsave', 'bitop', 'brpoplpush', 'client kill', 'client list', 'client getname',
    'client setname', 'config get', 'config set', 'config resetstat', 'dbsize', 'debug segfault', 'discard',
    'echo', 'eval', 'evalsha', 'exec', 'flushall', 'flushdb', 'info', 'lastsave', 'migrate', 'monitor',
    'msetnx', 'multi', 'object', 'ping', 'psubscribe', 'publish', 'punsubscribe', 'quit', 'randomkey',
    'rpoplpush', 'save', 'script exists', 'script flush', 'script kill', 'script load', 'sdiffstore', 'select',
    'shutdown', 'sinterstore', 'slaveof', 'slowlog', 'smove', 'subscribe', 'sunionstore', 'sync', 'time',
    'unsubscribe', 'unwatch', 'zinterstore', 'zunionstore'
  ];
  UNSHARDABLE.forEach((command) => {
    self[command] = () => {
      throw new Error(`${command} is not shardable`);
    };
  });

  // This is the tricky part - pipeline commands to multiple servers
  self.multi = function Multi() {
    const multiSelf = {};
    const multis = {};
    const interlachen = [];

    // Setup chainable shardable commands
    SHARDABLE.forEach((command) => {
      multiSelf[command] = (...args) => {
        const node = self.ring.get(args[0]);
        let multi = multis[node];
        if (!multi) {
          multis[node] = clients[node].multi();
          multi = multis[node];
        }
        interlachen.push(node);
        multi[command](...args);
        return multiSelf;
      };
    });

    UNSHARDABLE.forEach((command) => {
      self[command] = () => {
        throw new Error(`${command} is not supported`);
      };
    });

    // Exec the pipeline and interleave the results
    multiSelf.exec = (callback) => {
      const nodes = Object.keys(multis);
      step(
        function run() {
          const group = this.group();
          nodes.forEach((node) => {
            multis[node].exec(group());
          });
        },
        // eslint-disable-next-line consistent-return
        (error, groups) => {
          if (error) { return callback(error); }
          assert(nodes.length === groups.length, 'wrong number of responses');
          const results = [];
          interlachen.forEach((node) => {
            const index = nodes.indexOf(node);
            assert(groups[index].length > 0, `${node} is missing a result`);
            results.push(groups[index].shift());
          });
          callback(null, results);
        }
      );
    };
    return multiSelf; // Multi()
  };


  self.on = (event, listener) => {
    options.servers.forEach((server) => {
      clients[server].on(event, (...args) => {
        // append server as last arg passed to listener
        const largs = Array.prototype.slice.call(args).concat(server);
        listener(...largs);
      });
    });
  };

  // Note: listener will fire once per shard, not once per cluster
  self.once = (event, listener) => {
    options.servers.forEach((server) => {
      clients[server].once(event, (...args) => {
        // append server as last arg passed to listener
        const largs = Array.prototype.slice.call(args).concat(server);
        listener(...largs);
      });
    });
  };

  return self; // RedisShard()
};
