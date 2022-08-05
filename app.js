const config = require('config');
const cron = require('node-cron');
const { Client } = require('pg');
const logger = require('./logger');
const { jsonPrettyPrint } = require('./utils');

logger.info('Aplicação inicializada...');

async function rpsSequencer() {
    logger.info('Nova rotina inicializada...');

    const databaseConfig =  config.get('database.config');

    logger.debug(`Configurações da base de dados ${jsonPrettyPrint(databaseConfig)}`);

    const port = databaseConfig.port;
    const dbUser = databaseConfig.db_user;

    databaseConfig.databases.forEach(async (database) => {
        const db = new Client({
            user     : dbUser,
            host     : database.host,
            database : database.name,
            password : database.password,
            port     : port,
        });

        let { cnpj, garage, previousDays } = database;

        if (!previousDays) previousDays = 1;

        logger.info(`Dias retrocedidos para verificação: ${previousDays}`);

        try {
            await db.connect();

            logger.info("Buscando series a serem avaliadas...");

            const series = await getSeries(db, previousDays, garage);

            if (series.length < 1) {
                logger.info('Nenhuma operação a ser realizada....');
                return;
            }

            logger.debug(`Series a serem avaliadas : ${jsonPrettyPrint(series)}`);

             cnpj.forEach( (cnpjNumber) => {
                logger.info(`Iniciando validações do cnpj ${cnpjNumber}`);
                 series.forEach( (obj) => {
                    logger.info(`Teste para serie ${obj.serie} para o cnpj ${cnpjNumber}...`);

                    let needRepair =  testRps(db, obj, cnpjNumber, garage, previousDays);

                    logger.info(`Necessita de readjuste?: ${needRepair} >> ${obj.serie}, ${obj.numerorps}, ${cnpjNumber}`);

                    if (needRepair) {
                        for (let attempts = 0; attempts < 3; attempts++) {
                            logger.info(`Tentativa de rajuste ${attempts + 1}} de 3 para serie ${obj.serie}, ${obj.numerorps}`);

                            const result =  updateRps(db, obj, cnpjNumber, garage, previousDays);

                            logger.debug(`Retorno do update ${jsonPrettyPrint(result)}`);

                            logger.info('Verificando se ainda precisa de reajuste...');

                            let stillNeedRepair =  testRps(db, obj, cnpjNumber, garage, previousDays);

                            logger.info(`Precisa de reajuste ?: ${stillNeedRepair}}`);
                            logger.info('Sera efetuado uma nova tentativa...');

                            if (!stillNeedRepair) attempts = 4;
                        }
                    }
                });
            });
        } catch (err) {
            logger.fatal('Erro fatal...');
            logger.fatal(err.message);
            db.end();
        }
    });

    logger.info('Fechando conexao com banco de dados.');
    logger.info('Execução finalizada.');

    db.end();
}

 async function getSeries(db, previousDays, garage) {
    try {
        const result = await db.query(
            'SELECT ' +
                'DISTINCT serierps AS serie, ' +
                'MAX(numerorps) AS numerorps ' +
                'FROM recibo_provisorio_servicos ' +
                'WHERE data = (current_date - $1::interval) ' +
                'AND garagem = $2 ' +
                'GROUP BY serie ' +
                'ORDER BY serie',
            [`${previousDays+1}d`, garage]
        );

        return result.rows;
    } catch (err) {
        logger.error(`Erro ao recuperar series: ${err.message}`);
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
            [rpsObject.numerorps, `${previousDays}d`, garage, rpsObject.serie, cnpj]
        );

        if (result.rows.length < 1) return false;

        let sequences = result.rows;

        return !(sequences[sequences.length - 1].seq == sequences.length);
    } catch (err) {
        logger.erro(err.message);
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
            [rpsObject.numerorps, `${previousDays}d`, rpsObject.serie, garage, cnpj]
        );

        return result;
    } catch (err) {
        logger.error(err.message);
    }
}

cron.schedule(config.get('schedule'), () => rpsSequencer());
