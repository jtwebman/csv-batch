'use strict';

const Database = require('better-sqlite3');
const { Client: PgClient, Pool: PgPool } = require('pg');
const mysql = require('mysql2/promise');
const copyFrom = require('pg-copy-streams').from;
const fs = require('fs');
const path = require('path');

class DatabaseConnections {
  constructor() {
    this.connections = {};
  }

  async initSQLite(filename = 'benchmark.db') {
    const dbPath = path.join(__dirname, '..', 'results', filename);
    const db = new Database(dbPath);
    
    // Optimize for bulk inserts
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = OFF');
    db.pragma('cache_size = -2000000'); // 2GB cache
    db.pragma('temp_store = MEMORY');
    db.pragma('mmap_size = 30000000000'); // 30GB mmap
    
    // Create table
    db.exec(`
      CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        age INTEGER NOT NULL,
        salary INTEGER NOT NULL,
        department TEXT NOT NULL,
        created_at TEXT NOT NULL
      )
    `);
    
    // Prepare statements
    const insert = db.prepare(`
      INSERT INTO users (id, name, email, age, salary, department, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    
    const insertMany = db.transaction((records) => {
      for (const record of records) {
        insert.run(
          record.id,
          record.name,
          record.email,
          record.age,
          record.salary,
          record.department,
          record.created_at
        );
      }
    });
    
    this.connections.sqlite = {
      db,
      insert,
      insertMany,
      close: () => db.close()
    };
    
    return this.connections.sqlite;
  }

  async initPostgreSQL() {
    const pool = new PgPool({
      host: 'localhost',
      port: 5432,
      database: 'benchmark',
      user: 'bench',
      password: 'bench123',
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    });

    // Test connection
    const client = await pool.connect();
    
    // Create table
    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        age INTEGER NOT NULL,
        salary INTEGER NOT NULL,
        department VARCHAR(100) NOT NULL,
        created_at TIMESTAMP NOT NULL
      )
    `);
    
    client.release();
    
    this.connections.postgres = {
      pool,
      insertBatch: async (records) => {
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          
          // Build multi-row insert
          const values = [];
          const placeholders = [];
          let paramIndex = 1;
          
          for (const record of records) {
            const rowPlaceholders = [];
            for (let i = 0; i < 7; i++) {
              rowPlaceholders.push(`$${paramIndex++}`);
            }
            placeholders.push(`(${rowPlaceholders.join(',')})`);
            values.push(
              record.id,
              record.name,
              record.email,
              record.age,
              record.salary,
              record.department,
              record.created_at
            );
          }
          
          const query = `
            INSERT INTO users (id, name, email, age, salary, department, created_at)
            VALUES ${placeholders.join(',')}
          `;
          
          await client.query(query, values);
          await client.query('COMMIT');
        } catch (error) {
          await client.query('ROLLBACK');
          throw error;
        } finally {
          client.release();
        }
      },
      copyStream: async () => {
        const client = await pool.connect();
        const stream = client.query(copyFrom('COPY users FROM STDIN WITH (FORMAT csv, HEADER true)'));
        stream.on('end', () => client.release());
        stream.on('error', () => client.release());
        return stream;
      },
      close: () => pool.end()
    };
    
    return this.connections.postgres;
  }

  async initMySQL() {
    const pool = await mysql.createPool({
      host: 'localhost',
      port: 3306,
      database: 'benchmark',
      user: 'bench',
      password: 'bench123',
      waitForConnections: true,
      connectionLimit: 20,
      queueLimit: 0
    });

    // Create table
    await pool.execute(`
      CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        age INT NOT NULL,
        salary INT NOT NULL,
        department VARCHAR(100) NOT NULL,
        created_at DATETIME NOT NULL
      )
    `);
    
    this.connections.mysql = {
      pool,
      insertBatch: async (records) => {
        const values = records.map(r => [
          r.id,
          r.name,
          r.email,
          r.age,
          r.salary,
          r.department,
          // Convert ISO timestamp to MySQL DATETIME format
          new Date(r.created_at).toISOString().slice(0, 19).replace('T', ' ')
        ]);
        
        await pool.query(
          'INSERT INTO users (id, name, email, age, salary, department, created_at) VALUES ?',
          [values]
        );
      },
      close: () => pool.end()
    };
    
    return this.connections.mysql;
  }

  async closeAll() {
    for (const [name, conn] of Object.entries(this.connections)) {
      if (conn && conn.close) {
        await conn.close();
      }
    }
  }

  async truncateAll() {
    if (this.connections.sqlite) {
      this.connections.sqlite.db.exec('DELETE FROM users');
      this.connections.sqlite.db.exec('VACUUM');
    }
    
    if (this.connections.postgres) {
      const client = await this.connections.postgres.pool.connect();
      await client.query('TRUNCATE TABLE users');
      client.release();
    }
    
    if (this.connections.mysql) {
      await this.connections.mysql.pool.execute('TRUNCATE TABLE users');
    }
  }
}

module.exports = DatabaseConnections;