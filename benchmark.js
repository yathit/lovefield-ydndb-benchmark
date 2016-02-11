/**
 * Created by kyawtun on 10/2/16.
 */


var data = [];
var licenses = ['SA', 'CA', 'CC', 'NC'];
var publishers = ['AMC', 'Science', 'Nature', 'PlosOne', 'BMC', 'SAM', 'APress', 'Pocket'];
var years = [2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2009];
for (var i = 0; i < 50000; i++) {
	var d = i;
	var title = 'xxxxxxxx'.replace(/x/g, function(c) {
		var r = (d + Math.random() * 16) % 16 | 0;
		d = Math.floor(d / 16);
		return (c == 'x' ? r : (r & 0x3 | 0x8)).toString(16);
	});
	data.push({
		id: i,
		license: licenses[licenses.length * Math.random() | 0],
		publisher: publishers[publishers.length * Math.random() | 0],
		year: years[years.length * Math.random() | 0],
		title: title
	})
}

var disp = function(msg) {
	var p = document.createElement('p');
	p.textContent = msg;
	document.body.appendChild(p);
};
disp('Begin: ' + new Date().toLocaleTimeString());


// Lovefield
var schemaBuilder = lf.schema.create('benchmark-lf', 1);

schemaBuilder.createTable('article').
addColumn('id', lf.Type.INTEGER).
addColumn('license', lf.Type.STRING).
addColumn('publisher', lf.Type.STRING).
addColumn('year', lf.Type.INTEGER).
addColumn('title', lf.Type.STRING).
addPrimaryKey(['id']).
addIndex('licensetitle', ['license', 'title'], false, lf.Order.ASC).
addIndex('publishertitle', ['publisher', 'title'], false, lf.Order.ASC).
addIndex('yeartitle', ['year', 'title'], false, lf.Order.ASC);
var todoDb, article;
schemaBuilder.connect().then(function(db) {
	todoDb = db;
	article = db.getSchema().table('article');
	var rows = [];
	for (var i = 0; i < data.length; i++) {
		rows.push(article.createRow(data[i]));
	}
	return db.insertOrReplace().into(article).values(rows).exec();
}).then(function() {
	console.time('lf');
	return todoDb.select().from(article)
			.where(lf.op.and(article.license.eq('SA'), lf.op.and(article.publisher.eq('Science'), article.year.eq(2006))))
			.orderBy(article.title).limit(20).exec();
}).then(function(row) {
	console.timeEnd('lf');
	console.log(row);
	disp('Lovefield' + ' ' + new Date().toLocaleTimeString() + ' ' +
		row.length + ' articles from ' + row[0].title + ' to ' + row[row.length - 1].title);
});


// YDN-DB
var ydb = new ydn.db.Storage('benchmark-ydn', {
	stores: [{
		name: 'article',
		keyPath: 'id',
		indexes: [{
			keyPath: ['license', 'title']
		}, {
			keyPath: ['publisher', 'title']
		}, {
			keyPath: ['year', 'title']
		}, {
			keyPath: ['license', 'publisher', 'year', 'title']
		}]
	}]
});

ydb.putAll('article', data).done(function() {

	console.time('ydn');
	var iters = [ydn.db.IndexIterator.where('article', 'license, title', '^', ['SA']),
		ydn.db.IndexIterator.where('article', 'publisher, title', '^', ['Science']),
		ydn.db.IndexIterator.where('article', 'year, title', '^', [2006])];
	var match_keys = [];
	var solver = new ydn.db.algo.ZigzagMerge(match_keys, 20);
	var req = ydb.scan(solver, iters);
	req.then(function() {
		ydb.values('article', match_keys).done(function(row) {
			console.timeEnd('ydn');
			console.log(row);
			disp('YDN-DB' + ' ' + new Date().toLocaleTimeString() + ' ' +
				row.length + ' articles from ' + row[0].title + ' to ' + row[row.length - 1].title);

			console.time('ydn2');
			var kr = ydn.db.KeyRange.starts(['SA', 'Science', 2006]);
			ydb.valuesByIndex('article', 'license, publisher, year, title', kr, 20).done(function(arr) {
				console.timeEnd('ydn2');
				console.log(arr);
				disp('YDN-DB' + ' ' + new Date().toLocaleTimeString() + ' ' +
					arr.length + ' articles from ' + arr[0].title + ' to ' + arr[arr.length - 1].title);

			})

		});
	}, function(e) {
		console.error(e);
	});
});
