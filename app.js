const config = require('config');
const cron = require('node-cron');
const { Client } = require('pg');

async function rpsSequencer() {
    const databaseConfig = await config.get('database.config');
    console.log(databaseConfig);
    const port = databaseConfig.port;
    const dbUser = databaseConfig.db_user;

    databaseConfig.databases.forEach(async (database) => {
        const db = new Client({
            user: dbUser,
            host: database.host,
            database: database.name,
            password: database.password,
            port: port
        });

        const {cnpj, garage} = database;

        try {
            await db.connect();

            const series = await db.query(
                'SELECT '
                + 'DISTINCT serierps AS serie, '
                + 'MAX(numerorps) AS rps '
                + 'FROM recibo_provisorio_servicos '
                + "WHERE data = current_date - '2d'::interval "
                + 'AND garagem = $1 '
                + 'GROUP BY serie '
                + 'ORDER BY serie', [garage]);

            cnpj.forEach(async (cnpjNumber) => {
                await series.rows.forEach(async (obj) => {

                    let needRepair = await testRps(db, obj, cnpjNumber, garage);

                    console.log('need repair 41', needRepair);
                    if (needRepair) {
                        for(let attempts = 0; attempts < 3; attempts++ ) {
                            const updateResult = await db.query(
                                'UPDATE '
                                + 'recibo_provisorio_servicos '
                                + 'SET numerorps = ( SELECT COUNT(1) + $1 '
                                    + 'FROM recibo_provisorio_servicos rps '
                                    + 'WHERE rps.id <= recibo_provisorio_servicos.id '
                                    + "AND rps.data >= (current_date - '1d'::interval) "
                                    + 'AND serierps = $2 '
                                    + 'AND garagem = $3 '
                                    + 'AND cnpj_pagamento = $4::text ) '
                                + 'WHERE data >= current_date '
                                + 'AND serierps = $2 '
                                + 'AND garagem = $3 '
                                + 'AND cnpj_pagamento = $4::text', [obj.rps, obj.serie, garage, cnpjNumber]);

                            console.log('update result', updateResult.command);

                            let stillNeedRepair = await testRps(db, obj, cnpjNumber, garage);
                            console.log('still need repair 62', stillNeedRepair);
                            if (!stillNeedRepair)
                                attempts = 4;
                        }
                    }
                });
            });

        } catch(err) {
            console.log(err)
            db.end();
        }
    });
}

async function testRps(db, rpsObject, cnpj, garage) {
    const result = await db.query(
        'SELECT '
        + '( numerorps - $1 ) AS seq, '
        + 'numerorps, serierps '
        + 'FROM recibo_provisorio_servicos '
        + "WHERE data = (current_date - '1d'::interval)"
        + 'AND garagem = $2 '
        + 'AND serierps = $3 '
        + 'AND cnpj_pagamento = $4::text '
        + 'ORDER BY numerorps ASC', [rpsObject.rps, garage, rpsObject.serie, cnpj]);

    console.log(`Iniciando serie ${rpsObject.serie} para o cnpj ${cnpj}`);

    if (result.rows.length < 1)
        return false;

    let sequences = result.rows;
    console.log(result.rows);
    console.log(sequences[sequences.length - 1].seq, sequences.length);

    return  !(sequences[sequences.length - 1].seq == sequences.length);
}

cron.schedule(config.get('schedule'), () => rpsSequencer());
