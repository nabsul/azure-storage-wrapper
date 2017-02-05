const config = require( './config' );
const moment = require( 'moment' );
const Code = require( 'code' );
const Lab = require( 'lab' );

const storage = require( '../' );

const lab = exports.lab = Lab.script();

lab.experiment( 'table operations', () => {
	let client = null;
	const prefix = moment.utc().format( 'YYYYMMDDHHmmss' );

	lab.before( () => {
		client = new storage.TableClient( config.credentials );
		return client.iterateTables( ( t ) => client.deleteTable( t ) );
	} );

	lab.test( 'can connect to azure', () => {
		return client.listTablesSegmented()
			.then( ( result ) => {
				Code.expect( result ).to.be.an.object();
				Code.expect( result.entries ).to.be.an.array();
				Code.expect( result.entries.length ).to.equal( 0 );
				Code.expect( result.continuationToken ).not.to.be.undefined();
				Code.expect( result.continuationToken ).be.null();
			} );
	} );

	lab.test( 'can add a table', () => {
		const tableName = `test${prefix}`;
		return client.createTable( tableName )
			.then( ( result ) => {
				Code.expect( result.isSuccessful ).to.equal( true );
				Code.expect( result.statusCode ).to.equal( 204 );
				Code.expect( result.TableName ).to.equal( tableName );
			} );
	} );

	lab.test( 'can add an existing table', () => {
		return client.listTablesSegmented()
			.then( ( result ) => {
				Code.expect( result.entries ).to.be.an.array();
				Code.expect( result.entries.length ).to.equal( 1 );
				return client.createTableIfNotExists( result.entries[ 0 ] );
			} )
			.then( ( result ) => {
				Code.expect( result.isSuccessful ).to.equal( true );
				Code.expect( result.statusCode ).to.equal( 200 );
				Code.expect( result.created ).to.equal( false );
			} );
	} );

	lab.test( 'can delete a table', () => {
		return client.listTablesSegmented()
			.then( ( result ) => {
				Code.expect( result.entries ).to.be.an.array();
				Code.expect( result.entries.length ).to.equal( 1 );
				return client.deleteTable( result.entries[ 0 ] );
			} )
			.then( ( result ) => {
				Code.expect( result.isSuccessful ).to.equal( true );
				Code.expect( result.statusCode ).to.equal( 204 );
			} );
	} );

	lab.test( 'can check if table exists', () => {
		return client.createTable( `table1${prefix}` )
			.then( () => {
				return client.listTablesSegmented();
			} )
			.then( ( result ) => {
				return Promise.all( [
					client.doesTableExist( result.entries[ 0 ] ),
					client.doesTableExist( 'a' + result.entries[ 0 ] ),
				] );
			} )
			.then( ( results ) => {
				const exists = results[ 0 ];
				const notExists = results[ 1 ];
				Code.expect( exists.isSuccessful ).to.be.true();
				Code.expect( exists.statusCode ).to.equal( 200 );
				Code.expect( notExists.isSuccessful ).to.be.false();
				Code.expect( notExists.statusCode ).to.equal( 404 );
			} );
	} );

	lab.test( 'list tables with a prefix', () => {
		return client.createTable( `atable${prefix}` )
			.then( () => {
				return client.listTablesSegmented();
			} )
			.then( ( result ) => {
				Code.expect( result.entries.length ).to.equal( 2 );
				return client.listTablesSegmentedWithPrefix( 'at' );
			} )
			.then( ( result ) => {
				Code.expect( result.entries.length ).to.equal( 1 );
			} );
	} );

	lab.test( 'delete table if exists', () => {
		return client.listTablesSegmented()
			.then( ( result ) => {
				Code.expect( result.entries.length ).to.equal( 2 );
				return client.deleteTableIfExists( result.entries[ 0 ] );
			} )
			.then( ( result ) => {
				Code.expect( result ).to.be.true();
				return client.listTablesSegmented();
			} )
			.then( ( result ) => {
				Code.expect( result.entries.length ).to.equal( 1 );
				return client.deleteTableIfExists( 'sometable' );
			} )
			.then( ( result ) => {
				Code.expect( result ).to.be.false();
			} );
	} );
} );
