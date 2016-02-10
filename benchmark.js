/**
 * Created by kyawtun on 10/2/16.
 */


var data = [];
var licenses = ['SA', 'CA', 'CC', 'NC'];
var publishers = ['AMC', 'Science', 'Nature', 'PlosOne', 'BMC', 'SAM', 'APress', 'Pocket'];
var years = [2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2009];
for (var i = 0; i < 20000; i++) {
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

console.log('Begin', new Date().toLocaleTimeString());

// Lovefield, give a little head start ;-)
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
		rows.push(article.createRow({
			'id': 1,
			'description': 'Get a cup of coffee',
			'deadline': new Date(),
			'done': false
		}));
	}
	return db.insertOrReplace().into(article).values(rows).exec();
}).then(function() {
	return todoDb.select().from(article)
		.where(article.license.eq('SA')
			.and(article.publisher.eq('Science'))
			.and(article.year.eq(2006)))
		.orderBy(a.title).exec();
}).then(function(results) {
	results.forEach(function(row) {
		console.log('Lovefield', new Date().toLocaleTimeString(), row.length, row[0].title, row[row.length - 1].title, row);
	});
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
		}]
	}]
});

ydb.putAll('article', data);

var iters = [ydn.db.IndexIterator.where('article', 'license, title', '^', ['SA']),
	ydn.db.IndexIterator.where('article', 'publisher, title', '^', ['Science']),
	ydn.db.IndexIterator.where('article', 'year, title', '^', [2006])];
var match_keys = [];
var solver = new ydn.db.algo.ZigzagMerge(match_keys);
var req = ydb.scan(solver, iters);
req.then(function() {
	ydb.values('article', match_keys).done(function(row) {
		console.log('YDN-DB', new Date().toLocaleTimeString(), row.length, row[0].title, row[row.length - 1].title, row);
	});
}, function(e) {
	throw e;
});