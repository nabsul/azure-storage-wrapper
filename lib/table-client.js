const _ = require( 'lodash' );
const azureStorage = require( 'azure-storage' );
const util = require( './util' );

class TableClient {

	constructor( account, key ) {
		this.azureClient = azureStorage.createTableService( account, key );
	}

	createTableAsync( table ) {
		const func = ( callback ) => this.azureClient.createTableIfNotExists( table, callback );
		return util.wrapInPromise( func );
	}

	insertAsync( table, data ) {
		const func = ( cb ) => this.azureClient.insertEntity( table, util.toAzure( data ), cb );
		return util.wrapInPromise( func );
	};

	insertOrReplaceAsync( table, data ) {
		const func = ( cb ) => this.azureClient.insertOrReplaceEntity( table, util.toAzure( data ), cb );
		return util.wrapInPromise( func );
	};

	mergeAsync( table, data ) {
		const func = ( cb ) => this.azureClient.mergeEntity( table, util.toAzure( data ), cb );
		return util.wrapInPromise( func );
	};

	insertOrMergeAsync( table, data ) {
		const func = ( cb ) => this.azureClient.insertOrMergeEntity( table, util.toAzure( data ), cb );
		return util.wrapInPromise( func );
	};

	getAsync( table, pk, rk ) {
		const func = ( cb ) => this.azureClient.retrieveEntity( table, pk, rk, cb );
		return util.wrapInPromise( func )
			.then( fromAzure );
	};

	getQueryAsync( table, query, token ) {
		const func = ( cb ) => this.azureClient.queryEntities( table, query, token, cb );
		return util.wrapInPromise( func )
			.then( ( result ) => {
				result.entries = _.map( result.entries, fromAzure );
				return result;
			} );
	};

	iterateQueryAsync( table, query, iterator, token ) {
		return getQueryAsync( table, query, token )
			.then( ( result ) => {
				_.forEach( result.entries, iterator );
				if ( result.continuationToken ) {
					return this.iterateQueryAsync( table, query, iterator, result.continuationToken );
				}
			} );
	};

	getPartitionAsync( table, pk, token ) {
		const query = new azureStorage.TableQuery().where( 'PartitionKey eq ?', pk );
		return this.getQueryAsync( table, query, token );
	};

	iteratePartition( table, pk, iterator, token ) {
		return this.getPartitionAsync( table, pk, token )
			.then( ( result ) => {
				_.forEach( result.entries, iterator );
				if ( result.continuationToken ) {
					return this.iteratePartition( table, pk, iterator, result.continuationToken );
				}
			} );
	}

}

module.exports = TableClient;
