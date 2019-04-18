'use strict';

const aws = require('aws-sdk'),
	config = require('config'),
	logger = require('./logger');

aws.config.update(
	{
		accessKeyId: config.aws.accessKeyId,
		secretAccessKey: config.aws.secretAccessKey,
		region: config.aws.region
	}
);

const kinesis = new aws.Kinesis();

const putRecord = function putRecord(data, sequenceNumber, callback) {
	const params = {
		Data: JSON.stringify(data),
		PartitionKey: 'default',
		StreamName: config.kinesisStreamName,
		SequenceNumberForOrdering: sequenceNumber
	};

	//console.log(params.Data.length*2);
	//console.log(params.SequenceNumberForOrdering.length*2);
	//console.log(params.PartitionKey.length*2);
	//console.log(params.StreamName.length*2);
	return kinesis.putRecord(params, function (err, data) {
		if (err) {
			logger.fatal('Error in putting kinesis record', err);
			return callback(err);
		} else {
			return callback(null, data);  // successful response
		}
	});
};

module.exports = {
	putRecord: putRecord
};
