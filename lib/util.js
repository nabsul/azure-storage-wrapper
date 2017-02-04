const _ = require( 'lodash' );
const azureStorage = require( 'azure-storage' );

const wrapInPromise = ( callback ) => {
	return new Promise( ( fulfill, reject ) => {
		callback( ( error, result ) => {
			if ( error ) {
				return reject( error );
			}

			return fulfill( result );
		} );
	} );
};

const entGen = azureStorage.TableUtilities.entityGenerator;
const convertValue = ( value ) => {
	switch ( typeof value ) {
		case 'string':
			return entGen.String( value );
		case 'number':
			return ( value === parseInt( value ) ) ? entGen.Int64( value ) : entGen.Double( value );
		case 'boolean':
			return entGen.Boolean( value );
		case 'object':
			return entGen.String( JSON.stringify( value ) );
		default:
			throw new Error( 'Unknown type: ' + typeof value );
	}
};

const stripAzureFields = ( entity ) => {
	return _.omit( entity, [ 'PartitionKey', 'RowKey', 'Timestamp', '.metadata' ] );
};

const toAzure = ( entity ) => {
	return _.mapValues( entity, convertValue );
};

const convertFromAzureValue = ( value ) => {
	if ( ! value._ )
		return null;

	try {
		return JSON.parse( value._ );
	} catch ( error ) {
		return value._;
	}
};

const fromAzure = ( entity ) => {
	return _.mapValues( entity, convertFromAzureValue );
};

const iterateItemsAsync = ( queryFuncAsync, iterator, token ) => {
	return queryFuncAsync( token )
		.then( ( result ) => {
			const reducer = ( chain, task ) => chain.then( () => iterator( task ) );
			return _.reduce( result.entries, reducer, Promise.resolve() )
				.then( () => result );
		} )
		.then( ( result ) => {
			if ( ! result.continuationToken ) {
				return;
			}

			return iterateItemsAsync( queryFuncAsync, iterator, result.continuationToken );
		} );
};

module.exports = {
	wrapInPromise,
	toAzure,
	fromAzure,
	stripAzureFields,
	iterateItemsAsync,
};
