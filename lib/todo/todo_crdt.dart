import 'package:crdt/crdt.dart';

import 'database.dart';

class TodoCrdt extends Crdt<int, TodoDto> {
  final MyDatabase _db;

  @override
  final dynamic nodeId;

  TodoCrdt._create(this.nodeId) : _db = MyDatabase(nodeId);

  static Future<TodoCrdt> create<K, V>(dynamic nodeId) async {
    final m = TodoCrdt._create(nodeId);
    await m.init();
    return m;
  }

  @override
  Future<bool> containsKey(int key) async {
    return (await _db.getById(key)) != null;
  }

  Record<TodoDto> _make_record(Todo todo) {
    final hlc = Hlc.parse(todo.hlc);
    final hlc_modified = Hlc.parse(todo.modified_hlc);

    return Record<TodoDto>(hlc, todo.asDto(), hlc_modified);
  }

  @override
  Future<Record<TodoDto>?> getRecord(int key) async {
    final todo = await _db.getById(key);
    if (todo != null) {
      return _make_record(todo);
    }
    return null;
  }

  @override
  Future<void> purge() async {
    await _db.drop();
  }

  Todo _make_insertable(int key, Record<TodoDto> todo) {
    return Todo(
        id: key,
        title: todo.value!.title,
        content: todo.value!.content,
        hlc: todo.hlc.toString(),
        modified_hlc: todo.modified.toString(),
        modified_logical: todo.modified.logicalTime);
  }

  @override
  Future<void> putRecord(int key, Record<TodoDto> value) async {
    await _db.put(_make_insertable(key, value));
  }

  @override
  Future<void> putRecords(Map<int, Record<TodoDto>> recordMap) async {
    final records =
        recordMap.entries.map((entry) => _make_insertable(entry.key, entry.value)).toList();

    await _db.putAll(records);
  }

  @override
  Future<Map<int, Record<TodoDto>>> recordMap({Hlc? modifiedSince}) async {
    final rows = await _db.getSince(modifiedSince: modifiedSince);
    return {for (var row in rows) row.id: _make_record(row)};
  }

  @override
  Stream<MapEntry<int, TodoDto?>> watch({int? key}) {
    throw UnimplementedError('watch not implemented');
    //return _db.watch(key: key);
  }
}
