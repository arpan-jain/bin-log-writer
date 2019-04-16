const log4js = require('log4js');
const config = require('config');

const options = {
    appenders: {
        consoleLog: {
            type: 'console',
        },
        mailer: {
            type: '@log4js-node/smtp',
            recipients: config.mail.recipients,
            sender: 'error@rooter.io',
            subject: config.mail.subject,
            sendInterval: 0,
            transport: {
                plugin: 'smtp',
                options: {
                    host: 'smtp.gmail.com',
                    secure: true,
                    port: 465,
                    auth: {
                        user: config.mail.username,
                        pass: config.mail.password,
                    },
                    debug: false,
                },
            },
        },
        'just-err': {
            type: 'logLevelFilter',
            level: 'error',
            appender: 'mailer',
        },
    },
    categories: {
        default: {
            appenders:[
                'consoleLog',
                'just-err',
            ],
            level: 'info',
        },
    },
};
log4js.configure(options, {});
const logger = log4js.getLogger(config.loggerServiceName);

module.exports = logger;
