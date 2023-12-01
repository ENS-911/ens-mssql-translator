const sql = require('mssql');
const pgp = require('pg-promise')();
const dotenv = require('dotenv');
dotenv.config();

let data;

// AWS Lambda entry point
module.exports.handler = async (event, context) => {
  try {
    console.log('Incoming Event:', event);

    if (event && event.body) {
      const body = JSON.parse(event.body);
      console.log('Parsed Body:', body);

      // Your processing logic here using the 'body' variable

      return { statusCode: 200, body: 'Success' };
    } else {
      console.error('Invalid or empty body.');
      return { statusCode: 400, body: 'Invalid or empty body.' };
    }
  } catch (error) {
    console.error('Error processing Lambda event:', error);
    return { statusCode: 500, body: `Internal Server Error ***** The Data ${error.message}` };
  }

  try {
    // Parse JSON object from the event

    console.log(data.key)

    const dbName = `client-${data.key}`

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
        host: process.env.DB_HOST,
        database: dbName,
        password: process.env.DB_PASSWORD,
        port: process.env.DB_PORT,
    };

    // Connect to MSSQL
    await sql.connect(mssqlConfig);

    // Get data from MSSQL
    const mssqlQueryResult = await sql.query(`SELECT * FROM ${data.raw_table_name}`);

    // Connect to PostgreSQL
    const pgsql = pgp(pgsqlConfig);

    // Check if the table exists in PostgreSQL, create if not
    await pgsql.none(`CREATE TABLE IF NOT EXISTS client_data_${new Date().getFullYear()} (
        active character varying(3) COLLATE pg_catalog."default",
        agency_type character varying(64) COLLATE pg_catalog."default",
        battalion character varying(64) COLLATE pg_catalog."default",
        db_city character varying(64) COLLATE pg_catalog."default",
        creation character varying(64) COLLATE pg_catalog."default",
        crossstreets character varying(64) COLLATE pg_catalog."default",
        entered_queue character varying(64) COLLATE pg_catalog."default",
        db_id character varying(64) COLLATE pg_catalog."default",
        jurisdiction character varying(64) COLLATE pg_catalog."default",
        latitude character varying(64) COLLATE pg_catalog."default",
        location character varying(64) COLLATE pg_catalog."default",
        longitude character varying(64) COLLATE pg_catalog."default",
        master_incident_id character varying(64) COLLATE pg_catalog."default",
        premise character varying(64) COLLATE pg_catalog."default",
        priority character varying(64) COLLATE pg_catalog."default",
        sequencenumber character varying(64) COLLATE pg_catalog."default",
        stacked character varying(64) COLLATE pg_catalog."default",
        db_state character varying(64) COLLATE pg_catalog."default",
        status character varying(64) COLLATE pg_catalog."default",
        statusdatetime character varying(64) COLLATE pg_catalog."default",
        type character varying(64) COLLATE pg_catalog."default",
        type_description character varying(64) COLLATE pg_catalog."default",
        zone character varying(64) COLLATE pg_catalog."default"
    )`);

    // Set all rows' 'active' column to 'no'
    await pgsql.none(`UPDATE client_data_${new Date().getFullYear()} SET active = 'no'`);

    // Process each row from MSSQL
    for (const row of mssqlQueryResult.recordset) {
      // Check if the row exists in PostgreSQL
      const existingRow = await pgsql.oneOrNone(
        `SELECT * FROM client_data_${new Date().getFullYear()} WHERE mssql_column_key = $1`,
        [row.mssql_column_key]
      );

      if (existingRow) {
        // Row exists, update 'active' to 'yes'
        await pgsql.none(
          `UPDATE client_data_${new Date().getFullYear()} SET active = 'yes' WHERE mssql_column_key = $1`,
          [row.mssql_column_key]
        );
      } else {
        // Row doesn't exist, insert new row
        const columnNames = Object.keys(row).map((key) => data.translation[key] || key);
        const values = Object.values(row);

        await pgsql.none(
          `INSERT INTO client_data_${new Date().getFullYear()} (${columnNames.join(', ')}, active) VALUES (${'$1, '.repeat(
            columnNames.length
          )}$${columnNames.length + 1})`,
          [...values, 'active']
        );
      }
    }

    // Close connections
    await sql.close();
    pgp.end();

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