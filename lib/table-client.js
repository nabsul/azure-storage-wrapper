const _ = require( 'lodash' );
const azureStorage = require( 'azure-storage' );
const util = require( './util' );

class TableClient {

	constructor( storageAccountOrConnectionString, storageAccessKey, host, sasToken, endpointSuffix ) {
		this.azureClient = azureStorage.createTableService( storageAccountOrConnectionString, storageAccessKey, host, sasToken, endpointSuffix );
	}

	listTablesSegmented( currentToken, options ) {
		const func = ( cb ) => this.azureClient.listTablesSegmented( currentToken, options, cb );
		return util.wrapInPromise( func );
	}

	listTablesSegmentedWithPrefix( prefix, currentToken, options ) {
		const func = ( cb ) => this.azureClient.listTablesSegmentedWithPrefix( prefix, currentToken, options, cb );
		return util.wrapInPromise( func );
	}

	doesTableExist( table, options ) {
		const func = ( cb ) => this.azureClient.doesTableExist( table, options, cb );
		return util.wrapInPromise( func );
	}

	createTable( table, options ) {
		const func = ( cb ) => this.azureClient.createTable( table, options, cb );
		return util.wrapInPromise( func );
	}

	createTableIfNotExists( table, options ) {
		const func = ( cb ) => this.azureClient.createTableIfNotExists( table, options, cb );
		return util.wrapInPromise( func );
	}

	deleteTable( table, options ) {
		const func = ( cb ) => this.azureClient.deleteTable( table, options, cb );
		return util.wrapInPromise( func );
	}

	deleteTableIfExists( table, options ) {
		const func = ( cb ) => this.azureClient.deleteTableIfExists( table, options, cb );
		return util.wrapInPromise( func );
	}

	queryEntities( table, tableQuery, currentToken, options ) {
		const func = ( cb ) => this.azureClient.queryEntities( table, tableQuery, currentToken, options, cb );
		return util.wrapInPromise( func )
			.then( ( result ) => {
				result.entries = _.map( result.entries, util.fromAzure );
				return result;
			} );
	};

	retrieveEntity( table, partitionKey, rowKey, options ) {
		const func = ( cb ) => this.azureClient.retrieveEntity( table, partitionKey, rowKey, options, cb );
		return util.wrapInPromise( func )
			.then( util.fromAzure );
	};

	insertEntity( table, entity, options ) {
		const func = ( cb ) => this.azureClient.insertEntity( table, util.toAzure( entity ), options, cb );
		return util.wrapInPromise( func );
	};

	insertOrReplaceEntity( table, entity, options ) {
		const func = ( cb ) => this.azureClient.insertOrReplaceEntity( table, util.toAzure( entity ), options, cb );
		return util.wrapInPromise( func );
	};

	replaceEntity( table, entity, options ) {
		const func = ( cb ) => this.azureClient.replaceEntity( table, util.toAzure( entity ), options, cb );
		return util.wrapInPromise( func );
	}

	mergeEntity( table, entity, options ) {
		const func = ( cb ) => this.azureClient.mergeEntity( table, util.toAzure( entity ), options, cb );
		return util.wrapInPromise( func );
	};

	insertOrMergeEntity( table, entity, options ) {
		const func = ( cb ) => this.azureClient.insertOrMergeEntity( table, util.toAzure( entity ), options, cb );
		return util.wrapInPromise( func );
	};

	deleteEntity( table, entity, options ) {
		const func = ( cb ) => this.azureClient.deleteEntity( table, util.toAzure( entity ), options, cb );
		return util.wrapInPromise( func );
	}

	executeBatch( table, batch, options ) {
		const func = ( cb ) => this.azureClient.executeBatch( table, batch, options, cb );
		return util.wrapInPromise( func );
	}

	iterateTables( iterator, options ) {
		const queryFunc = ( token ) => this.listTablesSegmented( token, options );
		return util.iterateItemsAsync( queryFunc, iterator );
	}

	iterateTablesWithPrefix( prefix, iterator, options ) {
		const queryFunc = ( token ) => this.listTablesSegmentedWithPrefix( prefix, token, options );
		return util.iterateItemsAsync( queryFunc, iterator );
	}

	iterateQuery( table, query, iterator, options ) {
		const queryFunc = ( token ) => this.queryEntities( table, query, token, options );
		return util.iterateItemsAsync( queryFunc, iterator );
	};

	query() {
		return new azureStorage.TableQuery();
	};

}

module.exports = TableClient;
