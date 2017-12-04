#!/usr/bin/env node
'use strict';

/*** LIBRARY ***/

var fs = require('fs');
var moment = require('moment');
var readline = require('readline');
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
	var priority = [ "TEXT","BOOLEAN","FLOAT","INT","TIMESTAMP"/**,NULL**/];
	for(var t of priority)
	{
		if(types.indexOf(t) !== -1)
			return t;
	}
	return "VARCHAR(255)";
}

function parseValue(type, value)  {
	if(type == "BOOLEAN"){
		if(value == false)
			return false;
		else
			return true;
	}
	if(value == 'null')
		return "NULL";
	if(type == "TEXT")
		return "'"+value.replace(new RegExp("\'", 'g'), '´').slice(1,-1)+"'";
	if(type == "TIMESTAMP")
		return "'"+moment.unix(value).utc().format()+"'";
	if(type == "INT")
		return value.replace(new RegExp("\"", 'g'), '')
	return value;
}

function trimNewline(string) {
	while(true)
	{
		string = string.trim();
		if(string[string.length-1] == "\n"){
			string.slice(0,-1);
		}
		else{
			return string;
		}
	}
}

function splitObject(obj) {
	for(var key in obj) {
		
	}
}


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
var filecontent = "";
var tables = {};
var columns = {};
var json = [];


/*** JSONTOSQL ***/

function process_out(line)
{
	console.log(line);
}


function preformat(file, format) {
	if(format == "lines")
	{
		file = trimNewline(file);
		file = file.replace(new RegExp("\n", 'g'), ',');
		file = "[" + file + "]";
	}

	try {
		var json = JSON.parse(file);
	} catch(e) {
		console.error("Error parsing "+filename+" : "+e);
		return -1;
	}

	return json;
}

function table_obj(file, columns, format)
{
	var json = preformat(file,format);
	for(var obj of json)
	{
		for(var k in obj)
		{
			if(!columns[k])
				columns[k] = {};
			columns[k][sqlType(obj[k])] = true;
		}
	}
	return columns;
}


function table_sql(columns)
{
	for(var c in columns) {
		columns[c] = {type: reduceType(Object.keys(columns[c])), isNull: Object.keys(columns[c]).indexOf("NULL") != -1};
	}

	var create_table = `\n\nCREATE TABLE `+tablename+" (\n";

	create_table += Object.keys(columns).map(
			(key, index) => {
				return "\t"
				+ key
				+ " "
				+ columns[key]["type"]
				+ (columns[key]["isNull"]?"":" NOT NULL");
			}
		).join(",\n");

	create_table += "\n);\n\n"

	process_out(create_table);
	return columns;

}

function insert_sql(file, format)
{
	var json = preformat(file, format);
	for(var obj of json)
	{
		process_out(
			"INSERT INTO "
			+ tablename
			+ " (\n\t"
			+ Object.keys(obj).join(",\n\t")
			+ "\n) VALUES (\n\t"
			+ Object.keys(obj).map(k =>
				parseValue(columns[k].type, JSON.stringify(obj[k]))
			  ).join(",\n\t")
			+ "\n);\n"
		);
	}

}


/*** READING FILE ***/

try {

	var fileSize = fs.statSync(filename).size/(Math.pow(2,20));
	if(fileSize > 250){
		console.error("Reading big file ("+fileSize+" MiB)");
	}

	var lineNo = 0;

	/*** SCAN FOR TABLE COLUMNS ***/
	new readline.createInterface({
		input: fs.createReadStream(filename)
	})
	.on('line', (line) => {

		if(filecontent.length+line.length > Math.pow(2,25))
		{
			columns = table_obj(filecontent, columns, format);
			filecontent = "";
		}
		lineNo++;
		filecontent += (line+"\n");

	})
	.on('close', () => {

		columns = table_obj(filecontent, columns, format);
		filecontent = "";
		columns = table_sql(columns);


		/*** SCAN FOR TABLE CONTENT ***/
		new readline.createInterface({
			input: fs.createReadStream(filename)
		})
		.on('line', (line) => {

			if(filecontent.length+line.length > Math.pow(2,25))
			{
				insert_sql(filecontent, format);
				filecontent = "";
			}
			lineNo++;
			filecontent += (line+"\n");

		}).on('close', () => {
			insert_sql(filecontent, format);
		});

	});

} catch(e) {
	console.error("Error reading "+filename+" : "+e);
	return -1;
}



return 0;
