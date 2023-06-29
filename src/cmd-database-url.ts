import Config from '@kapeta/sdk-config';
import {createDBURI} from "./utils";


//Disable any logging from the SDK
console.log = function() {}

async function resolveUrl(resourceName: string) {
    const provider = await Config.init(process.cwd(), '' );
    return createDBURI(provider, resourceName);
}

if (!process.argv[2]) {
    console.error('Usage: resolve-postgres-url <resource-name>');
    process.exit(1);
}

resolveUrl(process.argv[2]).then(url => {
    process.stdout.write(url);
    process.exit(0);
}).catch(err => {
    console.error(err);
    process.exit(1);
});