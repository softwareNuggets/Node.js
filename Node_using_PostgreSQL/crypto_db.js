const { Pool } = require('pg');

const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: 'change_me',
  port: 5434,
});

async function insert_crypto_into_ledger(p_coin_name, price) {

  try {

    // Check for null parameters
    if (!p_coin_name || !price) {
      throw new Error('Parameters p_coin_name and price must not be null');
    }

    // Check minimum length for p_coin_name
    if (p_coin_name.length < 1) {
      throw new Error('Parameter p_coin_name must have a minimum length of 1');
    }

    const coin_name = p_coin_name.toUpperCase();

    // Acquire a client from the pool
    const client = await pool.connect();

    // Execute the insert_crypto_into_ledger function in the database
    // This function takes two parameters: symbol and price
    // It returns the ID of the newly inserted row
    const result = await client.query('SELECT insert_crypto_into_ledger($1, $2) as id', 
			[coin_name, price]);

    // Extract the returned ID from the result
    const id = result.rows[0].id;

    // Release the client back to the pool
    client.release();

    // Log the successful insertion
    console.log(`Inserted price for ${coin_name}: ${price}. New row ID: ${id}`);
  } catch (err) {
    // If an error occurs during the process, log it
    console.error('Error inserting price', err.stack);
  }
  // Note: We're not using 'finally' here to close the pool
  // This is likely because the pool is managed outside this function
  // and may be used for other operations
}

// Function to execute the PostgreSQL function and handle the results
async function getAllPrices2(v_coin_name) {
  try {
    // Use a client from the connection pool
    const client = await pool.connect();

    // Call the PostgreSQL function and fetch the results
    const { rows } = await client.query('SELECT * FROM get_all_prices($1)', [v_coin_name]);

    // Release the client back to the pool
    client.release();

    // Print the results in a table format
    console.log('ID\tCoin Name\t\tPrice\t\t\tDate');
    console.log('==============================================================');
    rows.forEach(row => {
      console.log(`${row.id}\t${row.coin_name}\t\t${row.price}\t${row.aodate}`);
    });

  } catch (err) {
    console.error('Error executing query', err);
  }
}

async function getCurrentPrice(symbol) {
  try {
    const client = await pool.connect();
    const result = await client.query('SELECT getCurrentPrice($1) as price', [symbol]);
    const price = result.rows[0].price;
    client.release();
    console.log(`Most recent price for ${symbol}: ${price}`);
  } catch (err) {
    console.error('Error getting price', err.stack);
  }
}

async function main() {
  const action = process.argv[2];
  const symbol = (process.argv[3] || '').toUpperCase();
  const price = process.argv[4];

  if (action === 'insert' && symbol && price) {
    await insert_crypto_into_ledger(symbol, parseFloat(price));
  } else if (action === 'get' && symbol) {
    await getCurrentPrice(symbol);
  } else if (action === 'table' && symbol) {
    await getAllPrices2(symbol);
  }  else {
	console.log('A solution from SoftwareNuggets');
	console.log('YouTube Channel  https://www.youtube.com/c/SoftwareNuggets');
	console.log('');
    console.log('Usage:');
    console.log('To insert new coin/price  : node crypto_db.js insert <symbol> <price>');
    console.log('To get single price       : node crypto_db.js get <symbol>');
    console.log('Get Datatable  all prices : node crypto_db.js table <symbol>');
  }

  await pool.end();
}

main().catch(console.error);
