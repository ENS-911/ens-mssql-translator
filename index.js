const sql = require('mssql');
const { Pool } = require('pg');
const dotenv = require('dotenv');
dotenv.config();

// AWS Lambda entry point
module.exports.handler = async (event, context) => {
    

    let data = event;

    console.log('Incoming Event parsed to data:', data);


    try {
        // Parse JSON object from the event
        const dbName = `client-${data.key}`;
        const dbLoc = data.trans_db_loc;

        console.log('DB Name', dbName);

        // Configure MSSQL connection
        const mssqlConfig = {
            user: data.raw_user,
            password: data.raw_pass,
            server: data.raw_server,
            database: data.raw_table,
        };

        // Configure PostgreSQL connection
        const pgsqlConfig = {
            user: process.env.DB_USER,
            host: dbLoc,
            database: 'postgres',
            password: "ZCK,tCI8lv4o",
            port: process.env.DB_PORT,
            max: 20,
            ssl: true,
        };

        // Connect to MSSQL
        await sql.connect(mssqlConfig);

        // Get data from MSSQL
        const mssqlQueryResult = await sql.query(`SELECT * FROM ${data.raw_table_name}`);

        console.log('mssql', mssqlQueryResult);

        // Create a PostgreSQL pool
        const pgPool = new Pool(pgsqlConfig);

    // Check if the table exists in PostgreSQL, create if not
    await pgPool.query(`CREATE TABLE IF NOT EXISTS client_data_${new Date().getFullYear()} (
        active VARCHAR(3),
        agency_type VARCHAR(64),
        battalion VARCHAR(64),
        db_city VARCHAR(64),
        creation VARCHAR(64),
        crossstreets VARCHAR(64),
        entered_queue VARCHAR(64),
        db_id VARCHAR(64),
        jurisdiction VARCHAR(64),
        latitude VARCHAR(64),
        location VARCHAR(64),
        longitude VARCHAR(64),
        master_incident_id VARCHAR(64),
        premise VARCHAR(64),
        priority VARCHAR(64),
        sequencenumber VARCHAR(64),
        stacked VARCHAR(64),
        db_state VARCHAR(64),
        status VARCHAR(64),
        statusdatetime VARCHAR(64),
        type VARCHAR(64),
        type_description VARCHAR(64),
        zone VARCHAR(64)
    )`);

    // Set all rows' 'active' column to 'no'
    await pgPool.query(`UPDATE client_data_${new Date().getFullYear()} SET active = 'no'`);

    // Process each row from MSSQL
    for (const row of mssqlQueryResult.recordset) {
        // Check if the row exists in PostgreSQL
        const existingRow = await pgPool.query(
            `SELECT * FROM client_data_${new Date().getFullYear()} WHERE mssql_column_key = $1`,
            [row.mssql_column_key]
        );

        if (existingRow.rows.length > 0) {
            // Row exists, update 'active' to 'yes'
            await pgPool.query(
                `UPDATE client_data_${new Date().getFullYear()} SET active = 'yes' WHERE mssql_column_key = $1`,
                [row.mssql_column_key]
            );
        } else {
            // Row doesn't exist, insert new row
            const columnNames = Object.keys(row).map((key) => data.translation[key] || key);
            const values = Object.values(row);

            await pgPool.query(
                `INSERT INTO client_data_${new Date().getFullYear()} (${columnNames.join(', ')}, active) VALUES ($${[
                    ...Array(columnNames.length + 1).keys(),
                ].join(', $')})`,
                [...values, 'active']
            );
        }
    }

    // Close connections
    await sql.close();
    await pgPool.end();

    return {
        statusCode: 200,
        body: JSON.stringify({ message: 'Successfully processed data' }),
    };
} catch (error) {
    console.error('Error:', error);
    return {
        statusCode: 500,
        body: JSON.stringify({ message: `Internal Server Error ***** The Data ${data}` }),
    };
}
};