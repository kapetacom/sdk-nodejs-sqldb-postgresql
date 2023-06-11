import Config from '@kapeta/sdk-config';

const RESOURCE_TYPE = 'kapeta/resource-type-postgresql';
const PORT_TYPE = 'postgres';

async function resolveUrl(resourceName: string) {
    const provider = await Config.init(process.cwd(), '' );
    const postgresInfo = await provider.getResourceInfo(RESOURCE_TYPE, PORT_TYPE, resourceName);
    const dbName =
        postgresInfo.options && postgresInfo.options.dbName
            ? postgresInfo.options.dbName
            : resourceName;

    let credentials = ''
    if (postgresInfo.credentials?.username) {
        credentials += postgresInfo.credentials.username;

        if (postgresInfo.credentials.password) {
            credentials += ':' + postgresInfo.credentials.password;
        }
    }

    return `postgresql://${credentials}@${postgresInfo.host}:${postgresInfo.port}/${dbName}`;
}

if (!process.argv[2]) {
    console.error('Usage: resolve-postgres-url <resource-name>');
    process.exit(1);
}

resolveUrl(process.argv[2]).then(url => {
    console.log(url);
    process.exit(0);
}).catch(err => {
    console.error(err);
    process.exit(1);
});