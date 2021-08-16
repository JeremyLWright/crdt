import 'dart:convert';
import 'dart:io';
import 'package:crdt/crdt.dart';
import 'package:moor/ffi.dart';
import 'package:path/path.dart' as p;
import 'package:moor/moor.dart';

// assuming that your file is called filename.dart. This will give an error at first,
// but it's needed for moor to know about the generated code
part 'database.g.dart';

class TodoDto {
  String title;
  String content;

  TodoDto({required this.title, required this.content});

  @override
  String toString() {
    return jsonEncode({'title': title, 'content': content});
  }

  String toJson() => toString();

  static TodoDto fromJson(String json) {
    final m = jsonDecode(json);
    return TodoDto(title: m['title'], content: m['content']);
  }
}

extension DtoExtensions on Todo {
  TodoDto asDto() {
    return TodoDto(title: title, content: content);
  }
}

// this will generate a table called "todos" for us. The rows of that table will
// be represented by a class called "Todo".
class Todos extends Table {
  IntColumn get id => integer().autoIncrement()();
  TextColumn get title => text().withLength(min: 6, max: 32)();
  TextColumn get content => text().named('body')();
  // IntColumn get category => integer().nullable()();
  //The HLC parts
  TextColumn get hlc => text()();
  TextColumn get modified_hlc => text()();
  IntColumn get modified_logical => integer()();
}

// This will make moor generate a class called "Category" to represent a row in this table.
// By default, "Categorie" would have been used because it only strips away the trailing "s"
// in the table name.
//@DataClassName('Category')
//class Categories extends Table {
//  IntColumn get id => integer().autoIncrement()();
//  TextColumn get description => text()();
//}

LazyDatabase _openConnection(String dbName) {
  // the LazyDatabase util lets us find the right location for the file async.
  return LazyDatabase(() async {
    // put the database file, called db.sqlite here, into the documents folder
    // for your app.
    final dbFolder = '.';
    final file = File(p.join(dbFolder, '$dbName.sqlite'));
    return VmDatabase(file);
  });
}

@UseMoor(tables: [Todos])
class MyDatabase extends _$MyDatabase {
  final String dbName;
  // we tell the database where to store the data with this constructor
  MyDatabase(this.dbName) : super(_openConnection(dbName));

  Future<Todo?> getById(int id) async {
    try {
      return (await (select(todos)..where((tbl) => tbl.id.equals(id))).get()).single;
    } catch (e) {
      return Future.value(null);
    }
  }

  Future<void> drop() async {
    await delete(todos).go();
  }

  Future<void> put(Todo todo) async {
    await into(todos).insert(todo, mode: InsertMode.replace);
  }

  Future<void> putAll(List<Todo> records) async {
    await batch((batch) => batch.insertAll(todos, records, mode: InsertMode.replace));
  }

  Future<List<Todo>> getSince({Hlc? modifiedSince}) async {
    return await (select(todos)
          ..where((tbl) => tbl.modified_logical.isBiggerThanValue(modifiedSince?.logicalTime ?? 0)))
        .get();
  }

  Stream<MapEntry<int, Todo?>> watch({int? key}) {
    if (key != null) {
      return (select(todos)..where((tbl) => tbl.id.equals(key)))
          .watchSingle()
          .map((event) => MapEntry(event.id, event));
    }
    throw UnimplementedError('Streaming all records is not implemented');
  }

  // you should bump this number whenever you change or add a table definition. Migrations
  // are covered later in this readme.
  @override
  int get schemaVersion => 1;
}
