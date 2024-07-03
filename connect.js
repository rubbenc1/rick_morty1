"use strict";
const { default: axios } = require("axios");
const fs = require("fs");
const { Client } = require("pg");
require("dotenv").config();

const config = {
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
    ssl: {
        ca: fs.readFileSync(`${process.cwd()}/.postgresql/root.crt`).toString(),
        rejectUnauthorized: true,
    },
};

const client = new Client(config);

async function fetchDataAndInsert(){
    try{
        await client.connect();
        console.log("Connected to the database!");

        //Delete the table if exists
        await client.query('DROP TABLE IF EXISTS rubbenc1');
        console.log("Table rubbenc1 dropped.")

        //Create a new table
        await client.query(`
            CREATE TABLE rubbenc1 (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            data JSONB NOT NULL
        )`);
        console.log('Table rubbenc1 created.');
        let url = process.env.API_URL
        while(url){
            try {
                const response = await axios.get(url);
                if(!response.data || !response.data.results){
                    throw new Error("No data found in the API response");
                }
                const data = response.data.results;
                const insertQuery = 'INSERT INTO rubbenc1 (name, data) VALUES($1, $2)';
                for (const item of data){
                    const name = item.name;
                    await client.query(insertQuery, [name, item])
                }
                console.log(`Data from ${url} inserted into the table.`);

                // Update the URL for the next page
                url = response.data.info.next;
            }catch(error){
                console.error(`Error fetching data from ${url}:`, error);
                break;
            }
        }
    }catch(err){
        console.error("Error connecting to PostgreSQL:", err);
    }finally {
        await client.end();
        console.log("Database connection closed.");
    }
}

fetchDataAndInsert();