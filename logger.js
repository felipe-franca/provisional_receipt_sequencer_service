const pino = require('pino');
const config = require('config');

module.exports = pino({
    enabled: config.get('logger.enabled') === 'true' ? true : false,
    level: config.get('logger.level'),
    transport: {
        target: 'pino-pretty',
        options: {
            translateTime: 'yyyy-dd-mm, H:MM:ss TT',
            destination: `logs/${new Date().toISOString()}.log`,
            mkdir: true,
            colorize: false,
        },
    },
});
