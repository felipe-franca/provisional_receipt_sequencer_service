const config = require('config');
const cron = require('node-cron');
const { Client } = require('pg');
const logger = require('./logger');

logger.info('Aplicação inicializada...');

async function rpsSequencer() {
    logger.info('Nova rotina inicializada !');

    const databaseConfig = await config.get('database.config');
    const port = databaseConfig.port;
    const dbUser = databaseConfig.db_user;

    databaseConfig.databases.forEach(async (database) => {
        const db = new Client({
            user: dbUser,
            host: database.host,
            database: database.name,
            password: database.password,
            port: port,
        });

        let { cnpj, garage, previousDays } = database;

        if (!previousDays) previousDays = 1;

        try {
            await db.connect();

            const series = await getSeries(db, previousDays, garage);

            cnpj.forEach(async (cnpjNumber) => {
                series.forEach(async (obj) => {
                    let needRepair = await testRps(db, obj, cnpjNumber, garage, previousDays);

                    if (needRepair) {
                        for (let attempts = 0; attempts < 3; attempts++) {
                            const result = await updateRps(db, obj, cnpjNumber, garage, previousDays);

                            let stillNeedRepair = await testRps(db, obj, cnpjNumber, garage, previousDays);

                            if (!stillNeedRepair) attempts = 4;
                        }
                    }
                });
            });
        } catch (err) {
            console.log(err);
            db.end();
        }
    });

    db.end();
}

async function getSeries(db, previousDays, garage) {
    try {
        const result = await db.query(
            'SELECT ' +
                'DISTINCT serierps AS serie, ' +
                'MAX(numerorps) AS rps ' +
                'FROM recibo_provisorio_servicos ' +
                'WHERE data = (current_date - $1::interval) ' +
                'AND garagem = $2 ' +
                'GROUP BY serie ' +
                'ORDER BY serie',
            [`${previousDays+1}d`, garage]
        );

        return result.rows;
    } catch (err) {
        console.log(err);
    }
}

async function testRps(db, rpsObject, cnpj, garage, previousDays) {
    try {
        const result = await db.query(
            'SELECT ' +
                '( numerorps - $1 ) AS seq, ' +
                'numerorps, serierps ' +
                'FROM recibo_provisorio_servicos ' +
                'WHERE data >= (current_date - $2::interval) ' +
                'AND garagem = $3 ' +
                'AND serierps = $4 ' +
                'AND cnpj_pagamento = $5::text ' +
                'ORDER BY numerorps ASC',
            [rpsObject.rps, `${previousDays}d`, garage, rpsObject.serie, cnpj]
        );

        if (result.rows.length < 1) return false;

        let sequences = result.rows;

        return !(sequences[sequences.length - 1].seq == sequences.length);
    } catch (err) {
        console.log(err);
    }
}

async function updateRps(db, rpsObject, cnpj, garage, previousDays) {
    try {
        const result = await db.query(
            'UPDATE ' +
                'recibo_provisorio_servicos ' +
                'SET numerorps = ( SELECT COUNT(1) + $1 ' +
                    'FROM recibo_provisorio_servicos rps ' +
                    'WHERE rps.id <= recibo_provisorio_servicos.id ' +
                    'AND rps.data >= ( current_date - $2::interval ) ' +
                    'AND serierps = $3 ' +
                    'AND garagem = $4 ' +
                    'AND cnpj_pagamento = $5::text ) ' +
                'WHERE data >= (current_date - $2::interval ) ' +
                'AND serierps = $3 ' +
                'AND garagem = $4 ' +
                'AND cnpj_pagamento = $5::text',
            [rpsObject.rps, `${previousDays}d`, rpsObject.serie, garage, cnpj]
        );

        return result;
    } catch (err) {
        console.log(err);
    }
}

cron.schedule(config.get('schedule'), () => rpsSequencer());
