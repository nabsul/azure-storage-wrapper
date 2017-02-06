const config = require( './config' );
const moment = require( 'moment' );
const Code = require( 'code' );
const Lab = require( 'lab' );

const storage = require( '../' );

const lab = exports.lab = Lab.script();

lab.experiment( 'table query operations', () => {
	let client = null;
	const prefix = moment.utc().format( 'YYYYMMDDHHmmss' );
	let tableName = null;

	lab.before( () => {
		tableName = `querytest${prefix}`;
		client = new storage.TableClient( config.credentials );
		return client.iterateTables( ( t ) => client.deleteTable( t ) )
			.then( () => {
				return client.createTable( tableName );
			} )
			.then( () => {
				const tasks = [
					client.insertEntity( tableName, { PartitionKey: 'p1', RowKey: 'r1', value: 1 } ),
					client.insertEntity( tableName, { PartitionKey: 'p1', RowKey: 'r2', value: 2 } ),
					client.insertEntity( tableName, { PartitionKey: 'p2', RowKey: 'r3', value: 3 } ),
				];

				return Promise.all( tasks );
			} );
	} );

	lab.test( 'can fetch non-existing item', () => {
		const items = [];
		const it = ( item ) => items.push( item );
		const query = client.query().where( 'PartitionKey eq ?', 'p3' );
		return client.iterateQuery( tableName, query, it )
			.then( () => {
				Code.expect( items.length ).to.equal( 0 );
				return client.iterateQuery( tableName, client.query().where( 'PartitionKey eq ?', 'p1' ), it );
			} )
			.then( () => {
				Code.expect( items.length ).to.equal( 2 );
				return client.iterateQuery( tableName, client.query().where( 'PartitionKey eq ?', 'p2' ), it );
			} )
			.then( () => {
				Code.expect( items.length ).to.equal( 3 );
			} );
	} );
} );
