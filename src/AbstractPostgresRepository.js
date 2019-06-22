const PostgresClient = require('./PostgresClient');

class AbstractPostgresRepository {

    /**
     * Initialise postgres client for database.
     *
     * @param {string} dbName
     */
    constructor(dbName) {
        this._dbName = dbName;
        this._client = new PostgresClient(dbName);
    }

    async findById(id) {

    }

    async findAll() {

    }

    async insert(entity) {

    }

    async update(entity) {

    }

    async deleteById(id) {

    }
}


module.exports = AbstractPostgresRepository;