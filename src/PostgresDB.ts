import Config, { ConfigProvider, ResourceInfo } from '@kapeta/sdk-config';
const RESOURCE_TYPE = 'kapeta/resource-type-postgresql';
const PORT_TYPE = 'postgres';

interface PrismaClient {
    $connect(): Promise<void>;
    $disconnect(): Promise<void>;
}


export abstract class PostgresDB<T extends PrismaClient> {
    private readonly _resourceName: string;
    private _ready: boolean = false;
    private _postgresInfo?: ResourceInfo;
    private _dbName?: string;
    private _prisma?: T;
    constructor(resourceName:string) {
        this._resourceName = resourceName;
        Config.onReady(async (provider) => {
            await this.init(provider);
        });
    }

    abstract createClient(opts: any): T;

    async init(provider: ConfigProvider) {
        this._postgresInfo = await provider.getResourceInfo(RESOURCE_TYPE, PORT_TYPE, this._resourceName);
        this._dbName =
            this._postgresInfo.options && this._postgresInfo.options.dbName
                ? this._postgresInfo.options.dbName
                : this._resourceName;

        let credentials = '';
        if (this._postgresInfo?.credentials?.username) {
            credentials += this._postgresInfo.credentials.username;

            if (this._postgresInfo.credentials.password) {
                credentials += ':' + this._postgresInfo.credentials.password;
            }
        }

        const url = `postgresql://${credentials}@${this._postgresInfo.host}:${this._postgresInfo.port}/${this._dbName}`;
        console.log('Connecting to postgres database: %s', url);

        this._prisma = this.createClient({
            datasources: {
                db: {
                    url
                },
            },
        });

        await this._prisma.$connect();
        console.log('Connected successfully to postgres database: %s', url);
        this._ready = true;
    }

    get client():T {
        if (!this._ready) {
            throw new Error('PostgresDB not ready');
        }

        return this._prisma!;
    }
}
