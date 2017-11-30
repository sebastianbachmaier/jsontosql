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
	var priority = [ "TEXT","FLOAT","INT","TIMESTAMP","BOOLEAN"/**,NULL**/];
	for(var t of priority)
	{
		if(types.indexOf(t) !== -1)
			return t;
	}
	return "VARCHAR(255)";
}

function parseValue(type, value) {
	if(value == 'null')
		return "NULL";
	if(type == "TEXT")
		return "'"+value.replace(new RegExp("\'", 'g'), '´').slice(1,-1)+"'";
	if(type == "TIMESTAMP")
		return "'"+moment.unix(value).utc().format()+"'";
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
var filecontent = [""];
/*** READING FILE ***/

try {

	var fileSize = fs.statSync(filename).size/(Math.pow(2,20));
	if(fileSize > 250){
		console.error("Reading big file ("+fileSize+" MiB)");
	}

	var lineNo = 0;

	new readline.createInterface({
		input: fs.createReadStream(filename)
	})
	.on('line', (line) => {
		
		if(filecontent[filecontent.length-1].length+line.length > Math.pow(2,20))
		{
			filecontent.push("");
		}
		lineNo++;
		filecontent[filecontent.length-1] += (line+"\n");

	})
	.on('close', () => {


		if(format == "lines")
		{
			for(var i in filecontent)
			{
				filecontent[i] = trimNewline(filecontent[i]);
				filecontent[i] = filecontent[i].replace(new RegExp("\n", 'g'), ',');
				filecontent[i] = "[" + filecontent[i] + "]";
			}
		}

		try {
			var json = JSON.parse(filecontent[0]);
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
					+ (columns[key]["isNull"]?"":" NOT NULL");
				}
			).join(",\n");

		create_table += "\n);"

		var insert_into = [];
		for(var obj of json)
		{
			insert_into.push(
				"INSERT INTO "
				+ tablename
				+ " (\n\t"
				+ Object.keys(obj).join(",\n\t")
				+ "\n) VALUES (\n\t"
				+ Object.keys(obj).map(k => 
					parseValue(columns[k].type, JSON.stringify(obj[k]))
				  ).join(",\n\t")
				+ ");\n"
			);
		}


		/*** PRINT ***/

		console.log(create_table);
		console.log(insert_into.join("\n"));



	});


} catch(e) {
	console.error("Error reading "+filename+" : "+e);
	return -1;
}



return 0;
