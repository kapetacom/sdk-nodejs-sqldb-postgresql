const Config = require('@blockware/sdk-config');
const { Pool, Client } = require('pg');

const RESOURCE_TYPE = "blockware/resource-type-postgresql";
const PORT_TYPE = "postgres";

class PostgresDB {

    /**
     * Initialise postgres client for database.
     *
     * @param {string} resourceName
     */
    constructor(resourceName) {
        this._resourceName = resourceName;
        this._dbName = null;

        this._ready = false;
        this._postgresInfo = null;
        this._pool = null;

        //Add init method to startup sequence
        Config.onReady(async (provider) => {
            await this.init(provider);
        });
    }

    /**
     * Called automatically during startup sequence.
     *
     * @param {LocalConfigProvider} provider
     * @return {Promise<void>}
     */
    async init(provider) {
        this._postgresInfo = await provider.getResourceInfo(RESOURCE_TYPE, PORT_TYPE, this._resourceName);
        this._dbName = this._postgresInfo.options && this._postgresInfo.options.dbName ? this._postgresInfo.options.dbName : this._resourceName;

        const dbUri = `${this._postgresInfo.host}:${this._postgresInfo.port}/${this._dbName}`;

        console.log('Connecting to postgres database: %s', dbUri);

        await this._ensureDatabase();

        this._pool = new Pool({
            host: this._postgresInfo.host,
            port: this._postgresInfo.port,
            database: this._dbName,
            user: this._postgresInfo.credentials.username,
            password: this._postgresInfo.credentials.password
        });

        await this._testConnection();

        console.log('Connected successfully to postgres database: %s', dbUri);
        this._ready = true;

    }

    /**
     * Creates database if it doesn't already exist
     *
     * @returns {Promise<void>}
     * @private
     */
    async _ensureDatabase() {
        let client;
        try {
            client = new Client({
                host: this._postgresInfo.host,
                port: this._postgresInfo.port,
                database: 'postgres',
                user: this._postgresInfo.credentials.username,
                password: this._postgresInfo.credentials.password
            });

            await client.connect();

            const result = await client.query(`SELECT 1 FROM pg_database WHERE datname = '${this._dbName}'`);

            if (result.rowCount > 0) {
                console.log("Using postgres database: %s",  this._dbName);
                return;
            }

            await client.query(`CREATE DATABASE "${this._dbName}"`);

            console.log("Created postgres database: %s", this._dbName);

        } finally {
            if (client) {
                client.end();
            }
        }
    }

    async _testConnection() {
        return  new Promise((resolve, reject) => {
            this._pool.connect((err,client,done)=>{
                if(err){
                    if (err.code === '3D000') {
                        //Database not found - create it
                        client.query('')
                    }
                    console.log('err', err);
                    console.log("Transaction error while testing the connection", err.stack);
                    done();
                    reject(err);
                    return;
                }
                client.query('SELECT NOW()', (err,res) => {
                    
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve();
                });
                client.release();
            })
        });
    }

    async query(sql) {
        const connection = await this._pool.connect();
        try {
            return connection.query(sql);
        } finally {
            await connection.release();
        }
    }

    async update(sql) {        
        const connection = await this._pool.connect();
        try {
            const result = await connection.query(sql);
            if (result &&
                result.rowCount !== undefined) {
                return result.rowCount;
            }
            return -1;
        } finally {
            await connection.release();
        }
    }

    async findRowById(tableName, id, primaryKey) {        
        return this.query(`SELECT * FROM "${tableName}" WHERE "${primaryKey}" = "${id}" LIMIT 1`);
    }
    
    async findAllRows(tableName) {
        return this.query(`SELECT * FROM "${tableName}"`);
    }
    
    async insertRow(tableName, entity) {
        const fields = Object.keys(entity);
        const values = Object.values(entity);
        
        return await this.update(`INSERT INTO "${tableName}" 
        ("${fields.join('","')}") VALUES ("${values.join('","')}")`);
        
    }
    
    async updateRow(tableName, entity, primaryKey) {
        const id = entity[primaryKey];
        const keys = Object.keys(entity).filter(key => key !== primaryKey);
        const assignments = keys.map((key) => {
            const value = entity[key];
            return `"${key}" = "${value}"`;
        });
        
        return await this.update(`
        UPDATE "${tableName}" SET 
        ${assignments.join(',\n')}
        WHERE "${primaryKey}" = "${id}" LIMIT 1`);
    }
    
    async deleteRowById(tableName, id, primaryKey) {
        return this.query(`DELETE FROM "${tableName}" WHERE "${primaryKey}" = "${id}" LIMIT 1`);
    }
}


module.exports = PostgresDB;