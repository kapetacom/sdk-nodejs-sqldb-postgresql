import Config, { ConfigProvider, ResourceInfo } from '@kapeta/sdk-config';
import { Client, Pool } from 'pg';

const RESOURCE_TYPE = 'kapeta/resource-type-postgresql';
const PORT_TYPE = 'postgres';

export type DBEntity = { [key: string]: string };

export class PostgresDB {
    private readonly _resourceName: string;
    private _ready: boolean = false;
    private _dbName?: string;
    private _postgresInfo?: ResourceInfo;
    private _pool?: Pool;

    /**
     * Initialise postgres client for database.
     */
    constructor(resourceName: string) {
        this._resourceName = resourceName;
        this._ready = false;

        //Add init method to startup sequence
        Config.onReady(async (provider) => {
            await this.init(provider);
        });
    }

    /**
     * Called automatically during startup sequence.
     */
    async init(provider: ConfigProvider) {
        this._postgresInfo = await provider.getResourceInfo(RESOURCE_TYPE, PORT_TYPE, this._resourceName);
        this._dbName =
            this._postgresInfo.options && this._postgresInfo.options.dbName
                ? this._postgresInfo.options.dbName
                : this._resourceName;

        const dbUri = `${this._postgresInfo.host}:${this._postgresInfo.port}/${this._dbName}`;

        console.log('Connecting to postgres database: %s', dbUri);

        await this._ensureDatabase();

        this._pool = new Pool({
            host: this._postgresInfo.host,
            port: parseInt('' + this._postgresInfo.port),
            database: this._dbName,
            user: this._postgresInfo.credentials?.username,
            password: this._postgresInfo.credentials?.password,
        });

        await this._testConnection();

        console.log('Connected successfully to postgres database: %s', dbUri);
        this._ready = true;
    }

    /**
     * Creates database if it doesn't already exist
     */
    async _ensureDatabase() {
        let client;
        const postgresInfo = this._postgresInfo;
        if (!postgresInfo) {
            throw new Error('Postgres not ready');
        }
        try {
            client = new Client({
                host: postgresInfo.host,
                port: parseInt('' + postgresInfo.port),
                database: 'postgres',
                user: postgresInfo.credentials!.username,
                password: postgresInfo.credentials!.password,
            });

            await client.connect();

            const result = await client.query(`SELECT 1
                                               FROM pg_database
                                               WHERE datname = '${this._dbName!}'`);

            if (result.rowCount > 0) {
                console.log('Using postgres database: %s', this._dbName);
                return;
            }

            await client.query(`CREATE DATABASE "${this._dbName!}"`);

            console.log('Created postgres database: %s', this._dbName);
        } finally {
            if (client) {
                await client.end();
            }
        }
    }

    async _testConnection() {
        return new Promise((resolve, reject) => {
            if (!this._pool) {
                reject(new Error('Postgres not ready'));
                return;
            }

            this._pool.connect(async (err: any, client, done) => {
                if (err) {
                    if (err['code'] === '3D000') {
                        //Database not found - create it
                        await client.query('');
                    }
                    console.log('err', err);
                    console.log('Transaction error while testing the connection', err.stack);
                    done();
                    reject(err);
                    return;
                }

                client.query('SELECT NOW()', (clientErr: any, res) => {
                    if (clientErr) {
                        reject(clientErr);
                        return;
                    }
                    resolve(null);
                });
                client.release();
            });
        });
    }

    async query(sql: string) {
        if (!this._pool) {
            throw new Error('Postgres not ready');
        }
        const connection = await this._pool.connect();
        try {
            return connection.query(sql);
        } finally {
            connection.release();
        }
    }

    async update(sql: string) {
        if (!this._pool) {
            throw new Error('Postgres not ready');
        }
        const connection = await this._pool.connect();
        try {
            const result = await connection.query(sql);
            if (result && result.rowCount !== undefined) {
                return result.rowCount;
            }
            return -1;
        } finally {
            connection.release();
        }
    }

    async findRowById(tableName: string, id: string, primaryKey: string) {
        return this.query(`SELECT *
                           FROM "${tableName}"
                           WHERE "${primaryKey}" = "${id}"
                           LIMIT 1`);
    }

    async findAllRows(tableName: string) {
        return this.query(`SELECT *
                           FROM "${tableName}"`);
    }

    async insertRow(tableName: string, entity: DBEntity) {
        const fields = Object.keys(entity);
        const values = Object.values(entity);

        return await this.update(`INSERT INTO "${tableName}"
                                      ("${fields.join('","')}")
                                  VALUES ("${values.join('","')}")`);
    }

    async updateRow(tableName: string, entity: DBEntity, primaryKey: string) {
        const id = entity[primaryKey];
        const keys = Object.keys(entity).filter((key) => key !== primaryKey);
        const assignments = keys.map((key) => {
            const value = entity[key];
            return `"${key}" = "${value}"`;
        });

        return await this.update(`
            UPDATE "${tableName}"
            SET ${assignments.join(',\n')}
            WHERE "${primaryKey}" = "${id}"`);
    }

    async deleteRowById(tableName: string, id: string, primaryKey: string) {
        return this.query(`DELETE
                           FROM "${tableName}"
                           WHERE "${primaryKey}" = "${id}"`);
    }
}
