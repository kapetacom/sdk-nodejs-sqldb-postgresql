const Config = require('@blockware/sdk-config');
const { Pool, Client } = require('pg');

const RESOURCE_TYPE = "sqldb.blockware.com/v1/postgresql";
const PORT_TYPE = "postgres";

class PostgresDB {

    /**
     * Initialise postgres client for database.
     *
     * @param {string} dbName
     */
    constructor(dbName) {
        this._dbName = dbName;
        
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
        this._postgresInfo = await provider.getResourceInfo(RESOURCE_TYPE, PORT_TYPE);

        this._pool = new Pool({
            host: this._postgresInfo.host,
            port: this._postgresInfo.port,
            database: this._dbName,
            user: this._postgresInfo.credentials.username,
            password: this._postgresInfo.credentials.password
        });

        await this._testConnection();
        this._ready = true;

    }

    async _testConnection() {
        return  new Promise((resolve, reject) => {
            this._pool.connect((err,client,done)=>{
                if(err){
                    console.log("Transaction error while testing the connection",err.stack);
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