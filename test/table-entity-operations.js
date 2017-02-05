const _ = require( 'lodash' );
const config = require( './config' );
const moment = require( 'moment' );
const Code = require( 'code' );
const Lab = require( 'lab' );

const storage = require( '../' );

const lab = exports.lab = Lab.script();

lab.experiment( 'table entity operations', () => {
	let client = null;
	const prefix = moment.utc().format( 'YYYYMMDDHHmmss' );
	let tableName = null;

	lab.before( () => {
		tableName = `entitytest${prefix}`;
		client = new storage.TableClient( config.credentials );
		return client.iterateTables( ( t ) => client.deleteTable( t ) )
			.then( () => {
				return client.createTable( tableName );
			} );
	} );

	lab.test( 'can fetch non-existing item', () => {
		return client.retrieveEntity( tableName, 'partition1', 'row1' )
			.then( ( result ) => {
				throw new Error( 'unexpected result: ' . JSON.stringify( result ) );
			} )
			.catch( ( error ) => {
				Code.expect( error ).to.be.an.error();
				Code.expect( error.message ).to.contain( 'resource does not exist' );
				Code.expect( error.code ).to.equal( 'ResourceNotFound' );
				Code.expect( error.statusCode ).to.equal( 404 );
			} );
	} );

	lab.test( 'can create an entity', () => {
		const entity = {
			PartitionKey: 'partition1',
			RowKey: 'row1',
			intValue: 12,
			doubleValue: 12.45,
			stringValue: 'hello',
		};

		return client.insertEntity( tableName, entity )
			.then( ( result ) => {
				Code.expect( result[ '.metadata' ] ).to.be.an.object();
				Code.expect( result[ '.metadata' ].etag ).to.be.a.string();

				return client.retrieveEntity( tableName, entity.PartitionKey, entity.RowKey );
			} )
			.then( ( result ) => {
				Code.expect( result ).to.include( entity );
				Code.expect( result.Timestamp ).to.be.a.date();
				Code.expect( result[ '.metadata' ] ).to.be.null();
			} );
	} );

	lab.test( 'can merge into a value', () => {
		let entity = null;
		return client.retrieveEntity( tableName, 'partition1', 'row1' )
			.then( ( result ) => {
				entity = _.omit( result, [ 'Timestamp', '.metadata' ] );
				Code.expect( result.doubleValue ).to.not.equal( 14.32 );
				entity.doubleValue = 14.32;
				entity.newValue = { str: 'valuehaha' };
				return client.mergeEntity( tableName, {
					PartitionKey: 'partition1',
					RowKey: 'row1',
					doubleValue: entity.doubleValue,
					newValue: entity.newValue,
				} );
			} )
			.then( ( result ) => {
				Code.expect( result[ '.metadata' ].etag ).to.be.a.string();
				return client.retrieveEntity( tableName, 'partition1', 'row1' );
			} )
			.then( ( result ) => {
				Code.expect( result ).to.contain( entity );
			} );
	} );
} );
