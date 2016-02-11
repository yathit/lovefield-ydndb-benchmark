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
var lfDb, ydnDb, alaDB;

// Lovefield
var loadLf = function() {
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
  return schemaBuilder.connect().then(function(db) {
    lfDb = db;
    article = db.getSchema().table('article');
    var rows = [];
    for (var i = 0; i < data.length; i++) {
      rows.push(article.createRow(data[i]));
    }
    return db.insertOrReplace().into(article).values(rows).exec();
  });
};

var runLf = function(db) {
    var article = db.getSchema().table('article');
    return lfDb.select().from(article)
        .where(lf.op.and(article.license.eq('SA'), lf.op.and(article.publisher.eq('Science'), article.year.eq(2006))))
        .orderBy(article.title).limit(20)
        .exec();
};

var loadYdn = function() {
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

  ydnDb = ydb;

  return ydb.putAll('article', data).done(function() {
    return ydb.values('article', null, 500000);
  })
};


var runYdn = function(ydb) {
	var iters = [ydn.db.IndexIterator.where('article', 'license, title', '^', ['SA']),
		ydn.db.IndexIterator.where('article', 'publisher, title', '^', ['Science']),
		ydn.db.IndexIterator.where('article', 'year, title', '^', [2006])];
	var match_keys = [];
	var solver = new ydn.db.algo.ZigzagMerge(match_keys, 20);
	return ydb.scan(solver, iters).done(function() {
		return ydb.values('article', match_keys);
	});
};

var runYdn2 = function(ydb) {
  var kr = ydn.db.KeyRange.starts(['SA', 'Science', 2006]);
  return ydb.valuesByIndex('article', 'license, publisher, year, title', kr, 20);
};

var runner = function(name, cb, db) {
  console.time(name);
  var start = +new Date();
  return cb(db).then(function(arr) {
    console.timeEnd(name);
    var taken = new Date() - start;
    var last = arr[arr.length - 1];
    disp(new Date().toLocaleTimeString() + ' ' + name + ': ' + taken + ' ms ' +
        arr.length + ' articles from ' + (arr[0].title || arr[0]) + ' to ' + (last.title || last));
    // console.log(arr);
  });
};

var loadAla = function(data) {
  var db = new alasql.Database('mybase');
  db.exec('CREATE TABLE article (id INT PRIMARY KEY, license STRING, publisher STRING, year INT, title STRING)');
  for (var i = 0; i < data.length; i++) {
    var row = data[i];
    db.exec('INSERT INTO article VALUES (?, ?, ?, ?, ?)', [row.id, row.license, row.publisher, row.year, row.title]);
  }
  alaDB = db;
  return Promise.resolve(db.exec('SELECT title FROM article'));
};

var runAla = function(db) {
  return Promise.resolve(db.exec('SELECT * FROM article WHERE license = ? AND publisher = ? AND year = ? ORDER BY title ASC LIMIT 20',
      ['SA', 'Science', 2006]));
};


runner('Load ALA', loadAla, data).then(function () {
  runner('Load YDN', loadYdn, data).then(function () {
    runner('Load LF', loadLf, data).then(function () {
      disp('Loaded ' + new Date().toLocaleTimeString());
      runner('LF', runLf, lfDb).then(function () {
        runner('YDN', runYdn, ydnDb).then(function () {
          runner('YDN2', runYdn2, ydnDb).then(function () {
            runner('ALA', runAla, alaDB).then(function () {
              disp('Done ' + new Date().toLocaleTimeString());
            });
          })
        })
      })
    })
  })
});

