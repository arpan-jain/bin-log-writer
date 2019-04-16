'use strict';

const redis = require('redis'),
    config = require('config'),
    logger = require('./logger.js');

const host = config.redis.host;
const port = config.redis.port;

logger.info('\nconnecting redis to: ', host, ':', port);

const redisCache = redis.createClient({
    host: host,
    port: port,
    retry_strategy: function(options) {
        console.log('retry options:', options);
        if (options.error && options.error.code === 'ECONNREFUSED') {
            // End reconnecting on a specific error and flush all commands with a individual error
            logger.fatal('redis connection refused received');
            return new Error('The server refused the connection');
        }

        if (options.error && options.error.code === 'ETIMEDOUT') {
            // End reconnecting on a specific error and flush all commands with a individual error
            logger.fatal('redis timeout received');
            return new Error('Connection is timing out from redis');
        }

        if (options.total_retry_time > 1000 * 5 * 60) {
            // End reconnecting after a specific timeout and flush all commands with a individual error
            return new Error('redis connection retry time exhausted');
        }

        if (options.times_connected > 3) {
            // End reconnecting with built in error
            logger.info('redis tried connecting 2 times. Couldn\'t connect');
            process.exit(0);
        }
        // reconnect after
        return Math.min(options.attempt * 100, 3000);
    }
});

redisCache.on('connect', function() {
});

redisCache.on('ready', function() {
    logger.info('redis is ready to accept connections');
    //app.emit('redisReady');
});

redisCache.on('error', function(err) {
    logger.fatal('redis error received',err);
    return setTimeout(function() {
        process.exit(0);
    },10000);
});

redisCache.on('reconnecting', function(err) {
    logger.info('reconnecting');
});


module.exports = redisCache;
