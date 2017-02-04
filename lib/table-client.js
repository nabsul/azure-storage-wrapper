const _ = require( 'lodash' );
const azureStorage = require( 'azure-storage' );
const util = require( './util' );

class TableClient {

	constructor( storageAccountOrConnectionString, storageAccessKey, host, sasToken, endpointSuffix ) {
		this.azureClient = azureStorage.createTableService( storageAccountOrConnectionString, storageAccessKey, host, sasToken, endpointSuffix );
	}

	getServiceStats( options ) {
		const func = ( cb ) => this.azureClient.getServiceStats( options, cb );
		return util.wrapInPromise( func );
	}

	getServiceProperties( options ) {
		const func = ( cb ) => this.azureClient.getServiceProperties( options, cb );
		return util.wrapInPromise( func );
	}

	setServiceProperties( serviceProperties, options ) {
		const func = ( cb ) => this.azureClient.setServiceProperties( serviceProperties, options, cb );
		return util.wrapInPromise( func );
	}

	listTablesSegmented( currentToken, options ){
		const func = ( cb ) => this.azureClient.listTablesSegmented( currentToken, options, cb );
		return util.wrapInPromise( func );
	}

	listTablesSegmentedWithPrefix( prefix, currentToken, options ){
		const func = ( cb ) => this.azureClient.listTablesSegmentedWithPrefix( prefix, currentToken, options, cb );
		return util.wrapInPromise( func );
	}

	getTableAcl( table, options ){
		const func = ( cb ) => this.azureClient.getTableAcl( table, options, cb );
		return util.wrapInPromise( func );
	}

	setTableAcl( table, signedIdentifiers, options ){
		const func = ( cb ) => this.azureClient.setTableAcl( table, signedIdentifiers, options, cb );
		return util.wrapInPromise( func );
	}

	generateSharedAccessSignature( table, sharedAccessPolicy ){
		return this.azureClient.generateSharedAccessSignature( table, sharedAccessPolicy );
	}

	doesTableExist( table, options ){
		const func = ( cb ) => this.azureClient.doesTableExist( table, options, cb );
		return util.wrapInPromise( func );
	}

	createTable( table, options ){
		const func = ( cb ) => this.azureClient.createTable(  table, options, cb );
		return util.wrapInPromise( func );
	}

	createTableIfNotExists( table, options ) {
		const func = ( cb ) => this.azureClient.createTableIfNotExists( table, options, cb );
		return util.wrapInPromise( func );
	}

	deleteTable( table, options ){
		const func = ( cb ) => this.azureClient.deleteTable( table, options, cb );
		return util.wrapInPromise( func );
	}

	deleteTableIfExists( table, options ){
		const func = ( cb ) => this.azureClient.deleteTableIfExists( table, options, cb );
		return util.wrapInPromise( func );
	}

	queryEntities( table, tableQuery, currentToken, options ) {
		const func = ( cb ) => this.azureClient.queryEntities( table, tableQuery, currentToken, options, cb );
		return util.wrapInPromise( func )
			.then( ( result ) => {
				result.entries = _.map( result.entries, fromAzure );
				return result;
			} );
	};

	retrieveEntity( table, partitionKey, rowKey, options ) {
		const func = ( cb ) => this.azureClient.retrieveEntity( table, partitionKey, rowKey, options, cb );
		return util.wrapInPromise( func )
			.then( fromAzure );
	};

	insertEntity( table, entity, options ) {
		const func = ( cb ) => this.azureClient.insertEntity( table, util.toAzure( entity ), options, cb );
		return util.wrapInPromise( func );
	};

	insertOrReplaceEntity( table, entity, options ) {
		const func = ( cb ) => this.azureClient.insertOrReplaceEntity( table, util.toAzure( entity ), options, cb );
		return util.wrapInPromise( func );
	};

	replaceEntity( table, entity, options ){
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

	deleteEntity( table, entity, options ){
		const func = ( cb ) => this.azureClient.deleteEntity( table, util.toAzure( entity ), options, cb );
		return util.wrapInPromise( func );
	}

	executeBatch( table, batch, options ){
		const func = ( cb ) => this.azureClient.executeBatch( table, batch, options, cb );
		return util.wrapInPromise( func );
	}

	iterateTables( iterator, options, token ) {
		return this.listTablesSegmented( token, options )
			.then( ( result ) => {
				_.forEach( result.entries, iterator );
				if ( result.continuationToken ) {
					return this.iterateTables( iterator, options,result.continuationToken );
				}
			} );
	}

	iterateTablesWithPrefix( prefix, iterator, options, token ) {
		return this.listTablesSegmentedWithPrefix( prefix, token, options )
			.then( ( result ) => {
				_.forEach( result.entries, iterator );
				if ( result.continuationToken ) {
					return this.iterateTables( prefix, iterator, options,result.continuationToken );
				}
			} );
	}

	iterateQuery( table, query, iterator, token ) {
		return this.queryEntities( table, query, token )
			.then( ( result ) => {
				_.forEach( result.entries, iterator );
				if ( result.continuationToken ) {
					return this.iterateQueryAsync( table, query, iterator, result.continuationToken );
				}
			} );
	};

}

module.exports = TableClient;
