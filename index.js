const sql = require('mssql');
const pgp = require('pg-promise')();

// AWS Lambda entry point
exports.handler = async (event, context) => {
  try {
    // Parse JSON object from the event
    const data = JSON.parse(event.body);

    // Configure MSSQL connection
    const mssqlConfig = {
      user: data.raw_user,
      password: data.raw_pass,
      server: data.raw_server,
      database: data.raw_table,
    };

    // Configure PostgreSQL connection
    const pgsqlConfig = {
      user: 'your-postgresql-username',
      password: 'your-postgresql-password',
      host: 'your-postgresql-host',
      port: 'your-postgresql-port',
      database: 'your-postgresql-database',
    };

    // Connect to MSSQL
    await sql.connect(mssqlConfig);

    // Get data from MSSQL
    const mssqlQueryResult = await sql.query('SELECT * FROM your_mssql_table');

    // Connect to PostgreSQL
    const pgsql = pgp(pgsqlConfig);

    // Check if the table exists in PostgreSQL, create if not
    await pgsql.none(`CREATE TABLE IF NOT EXISTS client_data_${new Date().getFullYear()} (...)`);

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
          [...values, 'yes']
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
      body: JSON.stringify({ message: 'Internal Server Error' }),
    };
  }
};