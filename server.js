'use strict';

// determining NODE_ENV
const environmentVariables = process.env;

const ZongJi = require('zongji'),
	config = require('config'),
	logger = require('./logger'),
	kinesis = require('./kinesisInitialize'),
	redisClient = require('./redisInitialize'),
	blocked = require('blocked'),
	zlib = require('zlib');

blocked(function (ms) {
	logger.info('BLOCKED FOR %sms', ms | 0);
});
require('longjohn');

let redisKey = config.offsetKey,   // key to hold current offset position
	binlogName = null,             // current bin log file name
	binlogPos = null,              // current bin log pos
	sequenceNumber = null,
	dbConfigObject = config.mysql,
	eventQueue = []; // mysql config

//var t1 = new Date();

/*var fetchAndPutEvent = function() {
    var currentEvent = eventQueue.shift(); // dequeue from the fifo queue

    if (currentEvent) {
        currentEvent = JSON.parse(currentEvent);
        // put in the kinesis stream with sequence number of last putRecord operation to achieve ordering of events
        return kinesis.putRecord(currentEvent, sequenceNumber, function(err, result) {
            if (err) {
                // in case of error while putting in kinesis stream kill the server and replay from the last successful offset
                logger.fatal('Error in putting kinesis record', err);
                return setTimeout(function() {
                    process.exit(0);
                }, 10000);
            }
            try {
                //store the binlog offset and kinesis sequence number in an external memory
                sequenceNumber = result.SequenceNumber;
                var offsetObject = {
                    binlogName: currentEvent.currentBinlogName,
                    binlogPos: currentEvent.currentBinlogPos,
                    sequenceNumber: sequenceNumber
                };
                redisClient.hmset(redisKey, offsetObject);
            }
            catch (ex) {
                logger.fatal('Exception in putting kinesis record', ex);
                setTimeout(function() {
                    process.exit(0);
                }, 10000);
            }
            return setImmediate(function() {
                return fetchAndPutEvent();
            });
        });
    }
    else {
        // in case of empty queue just recursively call the function again
        return setImmediate(function() {
            return fetchAndPutEvent();
        });
    }
};*/

const fetchAndPutEvent = function () {

	if (eventQueue.length) {
		let payload = [],
			payloadBuffer,
			payloadString,
			currentEvent;
		try {
			// aggregating the mysql events (the limit of payload is around 4 MB)
			while (eventQueue.length) {
				currentEvent = eventQueue.shift();        // dequeue from the eventQueue
				payload.push(currentEvent);               // pushing in the payload array
				// checking if the payload has increased the 4 Mb threshold or not (in js size =
				// string.length*2 Bytes)
				if (JSON.stringify(payload).length >= 2000000) {
					break;
				}
			}

			//console.log('size of object (in kB)', ((JSON.stringify(payload).length) * 2) / 1024);

			// compressing payload array through gzip (10 times reduced size, internal check sum)
			payloadBuffer = zlib.gzipSync(JSON.stringify(payload));
			// converting the compressed buffer to base64 string
			payloadString = payloadBuffer.toString('base64');

			//console.log('payloadString',payloadString);
			//console.log('size of buffer (in kB)', ((payloadString.length) * 2) / 1024);
			//console.log('\n\n');
		}
		catch (ex) {
			logger.fatal('Exception in fetching from eventQueue', ex);
			return setTimeout(function () {
				process.exit(0);
			}, 10000);
		}

		/*return zlib.gzip(JSON.stringify(payload),function(err, payloadBuffer){
				payloadString = payloadBuffer.toString('base64');
				console.log('size of buffer (in kB)', ((payloadString.length) * 2) / 1024);
				console.log('\n\n');
				return kinesis.putRecord(payloadString, sequenceNumber, function(err, result) {
						if (err) {
								// in case of error while putting in kinesis stream kill the server and replay from the last successful offset
								logger.fatal('Error in putting kinesis record', err);
								return setTimeout(function() {
										process.exit(0);
								}, 10000);
						}
						try {
								//store the binlog offset and kinesis sequence number in an external memory
								sequenceNumber = result.SequenceNumber;
								var offsetObject = {
										binlogName: currentEvent.currentBinlogName,
										binlogPos: currentEvent.currentBinlogPos,
										sequenceNumber: sequenceNumber
								};
								redisClient.hmset(redisKey, offsetObject);
								var t2 = new Date();
								console.log('diff', t2.getTime() - t1.getTime());
						}
						catch (ex) {
								logger.fatal('Exception in putting kinesis record', ex);
								setTimeout(function() {
										process.exit(0);
								}, 10000);
						}
						return setImmediate(function() {
								return fetchAndPutEvent();
						});
				});
		});*/
		return kinesis.putRecord(payloadString, function (err, result) {
			if (err) {
				// in case of error while putting in kinesis stream kill the server and replay from the
				// last successful offset
				logger.fatal('Error in putting kinesis record', err);
				return setTimeout(function () {
					process.exit(0);
				}, 10000);
			}
			try {
				sequenceNumber = result.SequenceNumber;
				const offsetObject = {
					binlogName: currentEvent.currentBinlogName,
					binlogPos: currentEvent.currentBinlogPos,
					sequenceNumber: sequenceNumber
				};
				redisClient.hmset(redisKey, offsetObject);
				//var t2 = new Date();
				//console.log('diff', t2.getTime() - t1.getTime());
			}
			catch (ex) {
				logger.fatal('Exception in putting kinesis record', ex);
				setTimeout(function () {
					process.exit(0);
				}, 10000);
			}
			return setImmediate(function () {
				return fetchAndPutEvent();
			});
		});

	}
	else {
		// in case of empty queue just recursively call the function again
		return setImmediate(function () {
			return fetchAndPutEvent();
		});
	}
};

const initialize = function () {

	// emptying the eventQueue to prevent duplicate elements on restart of this function
	eventQueue = [];

	logger.info('redisKey', redisKey);

	// initializing binlog name and binlog offset and sequence number
	redisClient.hgetall(redisKey, function (offsetObjectErr, offsetObject) {

		logger.info('offsetObject', offsetObject);
		if (offsetObjectErr) {
			logger.info('offsetObjectErr', offsetObjectErr);
			logger.fatal(environmentVariables.NODE_ENV.toUpperCase() + ' offsetObjectErr', offsetObjectErr);
			setTimeout(function () {
				process.exit(0);
			}, 10000);
		}
		if (offsetObject) {
			//initializing the binlogName
			if (offsetObject.binlogName && offsetObject.binlogName.length) {
				binlogName = offsetObject.binlogName;
			}
			//initializing the binlogPos
			if (offsetObject.binlogPos && offsetObject.binlogPos.length) {
				binlogPos = offsetObject.binlogPos;
			}
			//initializing the sequenceForOrdering
			if (offsetObject.sequenceNumber && offsetObject.sequenceNumber.length) {
				sequenceNumber = offsetObject.sequenceNumber;
			}
		}

		logger.info('dbConfigObject', dbConfigObject);
		// initializing the zongji server
		const zongji = new ZongJi(dbConfigObject);

		// binlog event listener
		zongji.on('binlog', function (evt) {

			try {

				//console.log('got the bin log event');
				/***
				 * tablemap and rotate events are required to get the updated binlog data, but these events
				 * are not to be pushed to kinesis
				 */

				if (evt.getEventName() === 'tablemap' || evt.getEventName() === 'rotate') {
					return;
				}

				let schema, tableName, rows, type, currentBinlogName, currentBinlogPos;

				if (evt.getEventName()) {
					type = evt.getEventName();
				}

				if (evt._zongji && evt._zongji.binlogName) {
					currentBinlogName = evt._zongji.binlogName;
				}
				if (evt._zongji && evt._zongji.binlogNextPos) {
					currentBinlogPos = evt._zongji.binlogNextPos;
				}
				if (evt.tableMap && evt.tableId && evt.tableMap[evt.tableId] && evt.tableMap[evt.tableId].parentSchema) {
					schema = evt.tableMap[evt.tableId].parentSchema;
				}
				if (evt.tableMap && evt.tableId && evt.tableMap[evt.tableId] && evt.tableMap[evt.tableId].tableName) {
					tableName = evt.tableMap[evt.tableId].tableName;
				}
				if (evt.rows) {
					rows = evt.rows;
				}

				const eventObject = {
					schema: schema,
					table: tableName,
					type: type,
					currentBinlogPos: currentBinlogPos,
					currentBinlogName: currentBinlogName,
					rows: rows
				};

				console.log(eventObject.schema + ' : ' + eventObject.table);
				return eventQueue.push(eventObject);
			}
			catch (ex) {
				logger.fatal('An exception Occurred', ex);
				setTimeout(function () {
					process.exit(0);
				}, 10000);
			}
		});

		zongji.on('error', function (err) {

			if (err.code !== 'PROTOCOL_CONNECTION_LOST') {
				logger.fatal('zongji error', err);
				setTimeout(function () {
					process.exit(0);
					initialize();
				}, 10000);
			}
			logger.info('zongji error', err);
			zongji.stop();
			// timeout to wait for mailer
			setTimeout(function () {
				//process.exit(0);
				initialize();
			}, 10000);
		});

		zongji.start({
			includeEvents: ['rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows'],
			binlogName: binlogName,
			binlogNextPos: binlogPos,
			serverId: config.zongjiServerId,
			includeSchema: {
				'rooter_feed': [
					'feed'
				]
			}
		});

		process.on('SIGINT', function () {
			logger.fatal('GOT SIGINT');
			setTimeout(function () {
				process.exit(0);
			}, 10000);
		});

		process.on('SIGTERM', function () {
			logger.fatal('GOT SIGINT');
			setTimeout(function () {
				process.exit(0);
			}, 10000);
		});

		process.on('exit', function () {
			logger.fatal('GOT process.exit');
			zongji.stop();
			setTimeout(function () {
			}, 20000);
		});
	});
};

initialize();
fetchAndPutEvent();


