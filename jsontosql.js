
/*** LIBRARY ***/

var fs = require('fs');
var moment = require('moment');
var readline = require('readline');
var Stream = require('stream');


moment.suppressDeprecationWarnings = true;



var JSON_TO_SQL = {

	/*** FUNCTIONS ***/

	sqlType: function(element) {
		if(element === null || element == "[deleted]"){
			return "NULL";
		}else if(element === true || element === false) {
			return "BOOLEAN";
		}else if(/^-?\d+\.\d+$/.test(element)){
			return "FLOAT";
		}else if(moment(element).isValid() && parseInt(element) > 100000000) {
			return "TIMESTAMP";
		}else if(/^-?\d+$/.test(element)){
			return "INT";
		}else{
			return "TEXT";
		}
	},

	reduceType: function(types) {
		var priority = [ "TEXT","BOOLEAN","FLOAT","INT","TIMESTAMP"/**,NULL**/];
		for(var t of priority)
		{
			if(types.indexOf(t) !== -1)
				return t;
		}
		return "VARCHAR(255)";
	},

	parseValue: function(type, value)  {
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
	},

	trimNewline: function(string) {
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
	},

	splitObject: function(obj) {
		for(var key in obj) {
			
		}
	},



	/*** JSONTOSQL ***/

	process_out: function(line, output)
	{
		output.stream.write(line);
	},


	preformat: function(file, format) {

		if(format == "lines")
		{
			file = JSON_TO_SQL.trimNewline(file);
			file = file.replace(new RegExp("\n", 'g'), ',');
			file = "[" + file + "]";
		}

		try {
			var json = JSON.parse(file);
		} catch(e) {
			console.error("Error parsing file : "+e);
			return -1;
		}

		return json;
	},

	table_obj: function(json, columns)
	{
		for(var obj of json)
		{
			for(var k in obj)
			{
				if(!columns[k])
					columns[k] = {};
				columns[k][JSON_TO_SQL.sqlType(obj[k])] = true;
			}
		}
		return columns;
	},


	table_sql: function(columns, output)
	{
		for(var c in columns) {
			columns[c] = {type: JSON_TO_SQL.reduceType(Object.keys(columns[c])), isNull: Object.keys(columns[c]).indexOf("NULL") != -1};
		}

		var create_table = `\n\nCREATE TABLE IF NOT EXISTS `+output.table+" (\n";

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

		JSON_TO_SQL.process_out(create_table, output);
		return columns;

	},

	insert_sql: function(json, columns, output)
	{
		for(var obj of json)
		{
			JSON_TO_SQL.process_out(
				"INSERT INTO "
				+ output.table
				+ " (\n\t"
				+ Object.keys(obj).join(",\n\t")
				+ "\n) VALUES (\n\t"
				+ Object.keys(obj).map(k =>
					JSON_TO_SQL.parseValue(columns[k].type, JSON.stringify(obj[k]))
				  ).join(",\n\t")
				+ "\n);\n"
			, output);
		}

	},

	stringStream: function() {

		Stream.call(this);
		this.writable = true;
		this["data"] = "";
		this.write = function(data) {
		    this["data"] += data;
		};

	},

	parse: function(json, stream) {

		if(typeof json === 'object' &&  !Array.isArray(json))
		{
			json = [json];
		}
		if(!stream){
			var mystream = new JSON_TO_SQL.stringStream();
		}
		var output = {
			table: "my_table",
			stream: mystream
		};
		var columns = {};
		var columns = JSON_TO_SQL.table_obj(json, columns);
		columns = JSON_TO_SQL.table_sql(columns, output);
		JSON_TO_SQL.insert_sql(json, columns, output);
		if(!stream){
			return mystream["data"];
		}
	},

	readFile: function(input, ops, callback, stream ) {
		
		var filename = input;
		var mystream = stream;
		var tablename = "my_table";
		var format = "standard";
		var filecontent = "";
		var tables = {};
		var columns = {};
		var fileSize = fs.statSync(filename).size/(Math.pow(2,20));

		if(!input){
			throw new Error ("filename not specified");
		}
		if(!mystream){
			mystream = new JSON_TO_SQL.stringStream();
		}
		if(ops){
			tablename = ops["table"];
			format = ops["format"];
		}
		if(fileSize > 250){
			console.error("Reading big file ("+fileSize+" MiB)");
		}

		var output = {
			table: tablename,
			stream: mystream
		};

		var lineNo = 0;

		/*** SCAN FOR TABLE COLUMNS ***/
		new readline.createInterface({
			input: fs.createReadStream(filename)
		})
		.on('line', (line) => {

			if(filecontent.length+line.length > Math.pow(2,25))
			{
				console.log(filecontent);
				var json = JSON_TO_SQL.preformat(filecontent, format);
				columns = JSON_TO_SQL.table_obj(json, columns);
				filecontent = "";
			}
			lineNo++;
			filecontent += (line+"\n");

		})
		.on('close', () => {

			var json = JSON_TO_SQL.preformat(filecontent, format);
			columns = JSON_TO_SQL.table_obj(json, columns);
			filecontent = "";
			columns = JSON_TO_SQL.table_sql(columns, output);


			/*** SCAN FOR TABLE CONTENT ***/
			new readline.createInterface({
				input: fs.createReadStream(filename)
			})
			.on('line', (line) => {

				if(filecontent.length+line.length > Math.pow(2,25))
				{
					var json = JSON_TO_SQL.preformat(filecontent, format);
					JSON_TO_SQL.insert_sql(json, columns, output);
					filecontent = "";
				}
				lineNo++;
				filecontent += (line+"\n");

			}).on('close', () => {
				var json = JSON_TO_SQL.preformat(filecontent, format);
				JSON_TO_SQL.insert_sql(json, columns, output);

				if(!stream){
					return callback(mystream["data"]);
				}else{
					return callback();
				}
				

			});

		});
	}

}

module.exports = JSON_TO_SQL;


