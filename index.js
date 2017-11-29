#!/usr/bin/env node
'use strict';

/*** LIBRARY ***/

var fs = require('fs');
var moment = require('moment');
moment.suppressDeprecationWarnings = true;
var ArgumentParser = require('argparse').ArgumentParser;


/*** FUNCTIONS ***/

function sqlType(element) {
	if(element === null || element == "[deleted]")
		return "NULL";
	if(element === true || element === false)
		return "BOOLEAN";
	if(moment(element).isValid() && parseInt(element) > 100000000)
		return "TIMESTAMP";
	if(/^-?\d+$/.test(element)){
		return "INT";
	}else if(/^-?\d+\.\d+$/.test(element)){
		return "DOUBLE";
	}else{
		return "TEXT";
	}
}

function reduceType(types) {
	var priority = [ "TEXT","FLOAT","INT","TIMESTAMP","BOOLEAN"/**,NULL**/];
	for(var t of priority)
	{
		if(types.indexOf(t) !== -1)
			return t;
	}
	return "VARCHAR(255)";
}

function parseValue(value) {
	if(value == "null")
		return "NULL";
	return value.replace(new RegExp("\'", 'g'), '´');;
}


/*** PARSING ARGUMENTS ***/

var argparse = new ArgumentParser({
	version: '1.0.0',
	addHelp: true,
	description: 'jsontosql'
});

argparse.addArgument(
	[ '-l', '--linewise' ],
	{
		action: 'storeTrue',
		help: 'json objects added linewise (not comma separated)'
	}
);

argparse.addArgument(
	[ '-f', '--file' ],
	{
		help: 'file'
	}
);

argparse.addArgument(
	[ '-t', '--table' ],
	{
		help: 'the sql table name'
	}
);

var args = argparse.parseArgs();

var filename = args["file"];
var tablename = args["table"];
var linewise = args["linewise"];

/*** READING FILE ***/

try {
	var filecontent = fs.readFileSync(filename, "utf8");
} catch(e) {
	console.error("Error reading "+filename+" : "+e);
	return -1;
}

if(linewise){
	filecontent = filecontent.replace(new RegExp("\n", 'g'), ',');
	filecontent = "[" +filecontent + "]";
}


try {
	var json = JSON.parse(filecontent);
} catch(e) {
	console.error("Error parsing "+filename+" : "+e);
	return -1;
}

/*** READING COLUMNS ***/


var columns = {};
for(var obj of json)
{
	for(var k in obj)
	{
		if(!columns[k])
			columns[k] = {};
		columns[k][sqlType(obj[k])] = true;
	}
}

for(var c in columns) {
	columns[c] = {type: reduceType(Object.keys(columns[c])), isNull: Object.keys(columns[c]).indexOf("NULL") != -1};
}


/*** CREATE STRINGS ***/

var create_table = "CREATE TABLE "+tablename+" (\n";

create_table += Object.keys(columns).map(
		(key, index) => {
			return "\t"
			+ key
			+ " "
			+ columns[key]["type"]
			+ " NOT NULL";
		}
	).join(",\n");

create_table += "\n);"

var insert_into = [];
for(var obj of json)
{
	insert_into.push(
		"INSERT INTO "
		+ tablename
		+ " ("
		+ Object.keys(obj).join(",")
		+ ") VALUES ("
		+ Object.keys(obj).map(k => 
			"\'"+parseValue(obj[k]+"")+"\'"
		  ).join(",")
		+ ");"
	);
}


/*** PRINT ***/

console.log(create_table);
console.log(insert_into.join("\n"));


return 0;
