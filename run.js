#!/usr/bin/env node
'use strict';

/*** LIBRARY ***/
var ArgumentParser = require('argparse').ArgumentParser;
var jsontosql = require('./jsontosql')


/*** PARSING ARGUMENTS ***/

var argparse = new ArgumentParser({
	version: '1.0.0',
	addHelp: true,
	description: 'jsontosql'
});

argparse.addArgument(
	[ '-f', '--format' ],
	{
		help: `format of json file : 


	standard  
		[
			{...},
			{...},
			 ...
		]
	lines 
		{...}\\n{...}\\n...
`
	}
);

argparse.addArgument(
	[ '-i', '--input' ],
	{
		help: 'the filename'
	}
);

argparse.addArgument(
	[ '-t', '--table' ],
	{
		help: 'the sql table name'
	}
);


var args = argparse.parseArgs();

var filename = args["input"];
var tablename = args["table"];
var format = args["format"];


/*** READING FILE ***/

try {
	jsontosql.readFile(filename, {table: tablename, format: format}, () => {
		
	},  process.stdout);

} catch(e) {
	console.error("Error reading "+filename+" : "+e);
	return -1;
}



return 0;
