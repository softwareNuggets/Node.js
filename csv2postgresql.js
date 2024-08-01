// software nuggets => https://www.youtube.com/c/softwareNuggets
// written by : scott johnson
// source at github => https://github.com/softwareNuggets/Node.js/blob/main/csv2postgresql.js

// Import necessary modules
const fs          = require('fs'); // For file read stream
const fsp         = require('fs').promises; // For promise-based file operations
const csv         = require('csv-parser'); // Needed to parse CSV files into a usable format
const readline    = require('readline');
const { Client }  = require('pg'); // Needed to interact with the PostgreSQL database

// PostgreSQL client configuration
const client = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: 'postgres',
  port: 5434,
});

// Define the file path variable
const filePath = 'C:/temp/coins.csv';

// Function to insert data into the Ledger table
async function insertIntoLedger(coin_name, price, aodate) {
  try {

    // Execute the SQL function to insert data into the Ledger table
    const res = await client.query(
		'SELECT insert_coins_from_csv_into_ledger($1, $2, $3)'
		,[coin_name, price, aodate]);

    console.log(`Inserted record with ID: 
		${res.rows[0].insert_coins_from_csv_into_ledger}`);

  } catch (err) {
    console.error('Error inserting data into Ledger:', err);
  }
}

async function processCSV(filePath) {
  const rows = [];
  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  let isFirstLine = true;

  for await (const line of rl) {
    if (isFirstLine) {
      isFirstLine = false;
      continue; // Skip the header line
    }

    // Remove outer quotes if present
    const trimmedLine = line.trim().replace(/^'|'$/g, '');

    // Split the line into parts
    const parts = trimmedLine.split(',');

    if (parts.length == 3) {
      const coin_name = parts[0].trim().replace(/^'|'$/g, '');
      const price = parts[1].trim();
      const aodate = parts.slice(2).join(',').trim().replace(/^'|'$/g, '');

      // all fields are required, if any length==0, skip that row
      if (coin_name.length==0 || price.length==0 || aodate.length==0) {
          console.log("skipping row, all fields are manatory: ");
          continue;
      }

      // Build new row
      const newRow = `'${coin_name}',${price},'${aodate}'`;
      rows.push(newRow);

    } else {
      console.log('Skipping invalid row:', line);
    }
  }

  return rows;
}


// Function to check if the file exists
async function fileExists(file) {
  try {
    // Check if the file exists
    await fsp.access(file);
    console.log(`The file "${file}" exists.`);
    return true;
  } catch (err) {
    console.log(`The file "${file}" does not exist.`);
    console.log(err);
    return false;
  }
}

// Main function to orchestrate the CSV processing and data insertion
async function main() {
  try {
    // Connect to the PostgreSQL database
    await client.connect();
    console.log('Connected to the PostgreSQL database');

    const csvFilePath = filePath;

    if (await fileExists(csvFilePath)) {

      const rows = await processCSV(csvFilePath);

      for (const row of rows) {

        const [coin_name, price, aodate] = row.split(',').map(item => item.trim().replace(/^'|'$/g, ''));

        if (coin_name && price && aodate) {
          console.log(`Processing coin: ${coin_name}, price: ${price}, date: ${aodate}`);
          
          try {
            await insertIntoLedger(coin_name, price, aodate);
          } catch (error) {
            console.error('Error inserting into ledger:', error);
          }
        } else {
          console.log('Invalid data in row:', {coin_name, price, aodate});
        }
      }
    } else {
      console.log('CSV file not found');
    }
  } catch (err) {
    console.error('Error processing CSV file:', err);
  } finally {
    await client.end();
    console.log('Disconnected from the PostgreSQL database');
  }
}

// Run the main function to start the process
main();
