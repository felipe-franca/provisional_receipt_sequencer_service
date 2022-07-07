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
                + "WHERE data = current_date - '1d'::interval "
                + 'AND garagem = ' + garage + ' '
                + 'GROUP BY serie '
                + 'ORDER BY serie'
            );

            cnpj.forEach(async (cnpj) => {
                await series.rows.forEach(async (obj) => {
                    const result = await db.query(
                        'SELECT '
                        + '( numerorps - ' + obj.rps +' ) AS seq, '
                        + 'numerorps, serierps '
                        + 'FROM recibo_provisorio_servicos '
                        + 'WHERE data = current_date '
                        + 'AND garagem = ' + garage + ' '
                        + 'AND serierps = ' + obj.serie + ' '
                        + "AND cnpj_pagamento = '" + cnpj + "' "
                        + 'ORDER BY numerorps ASC'
                    );

                    console.log('iniciando serie ', obj.serie);
                    console.log(result.rows);
                });
            });

        } catch(err) {
            console.log(err)
            db.end();
        }
    });

    return true;
}

cron.schedule(config.get('schedule'), () => rpsSequencer());
