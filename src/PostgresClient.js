const Config = require('@blockware/sdk-config');
const { Pool, Client } = require('pg');

const RESOURCE_TYPE = "sqldb.blockware.com/v1/postgresql";
const PORT_TYPE = "postgres";

class PostgresClient {

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
     * @param {ConfigProvider} provider
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

        console.log('Postgres client ready for %s --> %s:%s', this._dbName, this._postgresInfo.host, this._postgresInfo.port);
    }

    async _testConnection() {
        return new Promise((resolve, reject) => {
            this._pool.query('SELECT NOW()', (err) => {
                this._pool.end();
                if (err) {
                    reject(err);
                    return;
                }
                resolve();
            });
        });
    }

    async query(sql) {

    }

    async update(sql) {

    }
}


module.exports = PostgresClient;