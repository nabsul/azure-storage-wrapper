# Azure Storage Async Wrapper

## Intro

This class wraps the `azure-storage` module and brings you an easier to use interface. 
Currently, the focus is to:

- Support both callback functions and promises
- Automatically convert between Azure table entity objects to and from regular flat objects
- Hide token requests through the use of iterators

## Promises and Callbacks

If the callback function is not provided, the function will now return a promise.
You can therefore do the following:

```javascript
tableClient.retrieveEntity( table, partitionKey, rowKey, ( error, result ) => {
	if ( error ) {
		return console.log( error );
	}
	console.log( result );
} );

tableClient.retrieveEntity( table, partitionKey, rowKey )
    .then( ( result ) => console.log( result ) )
    .catch( ( error ) => console.log( error ) );
```

They both work, just pick your poison! :-)

## Simplified Table Entities

Azure table storage entities look like this:
 
```javascript
{
    dueDate: {"_": new Date(), "$":"Edm.DateTime" }
}
```

This library hides that so that you can simply do this:

```javascript
{
    dueDate: new Date()
}
```

Note: There is a certain loss of control for the sake of simplicity here. 
For some specialized applications, this might not be ideal.

If you need to access the raw entity data directly, you can do this:

```javascript
tableClient.queryEntities(); // returns simplified objects
tableClient.raw.queryEntityes(); // returns regular azure table object
```

## Query Iterators

For queries that result in large sets of data, Azure returns a continuation token. 
You must then make another API request using the token to receive the next page of results. 
Iterator methods wrap this by taking an iterator function that is run once for each result entry. 

```javascript
const iterator = ( entity ) => console.log( entity );

// You can go over the data this way:
const getAll = ( table, token ) => {
	tableClient.queryEntities( table, null, token )
	    .then( ( result ) => {
	    	result.entries.forEach( iterator );
	    	if( result.continuationToken )
	    		return getAll( table, result.continuationToken );
	    } );
};
getAll( 'MyAzureTable' );

// Or you can do it this way:
tableClient.iterateEntities( 'MyAzureTable', iterator );
```
