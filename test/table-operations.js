const config = require( './config' );
const moment = require( 'moment' );
const Code = require( 'code' );
const Lab = require( 'lab' );

const storage = require( '../' );

const lab = exports.lab = Lab.script();

lab.experiment( 'basic connection', () => {
	let client = null;
	const prefix = moment.utc().format( 'YYYYMMDDHHmmss' );

	lab.before( () => {
		client = new storage.TableClient( config.credentials );
		return client.iterateTables( ( t ) => client.deleteTable( t ) );
	} );

	lab.test( 'can connect to azure', () => {
		return client.listTablesSegmented()
			.then( ( result ) => {
				console.dir( result );
				Code.expect( result ).to.be.an.object();
				Code.expect( result.entries ).to.be.an.array();
				Code.expect( result.entries.length ).to.equal( 0 );
				Code.expect( result.continuationToken ).not.to.be.undefined();
				Code.expect( result.continuationToken ).be.null();
			} );
	} );

	lab.test( 'can add a table', () => {
		const tableName = `test${prefix}`;
		console.log( tableName );
		return client.createTable( tableName )
			.then( ( result ) => {
				Code.expect( result.isSuccessful ).to.equal( true );
				Code.expect( result.statusCode ).to.equal( 204 );
				Code.expect( result.TableName ).to.equal( tableName );
			} );
	} );
} );
