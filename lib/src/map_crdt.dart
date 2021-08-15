import 'dart:async';

import 'crdt.dart';
import 'hlc.dart';
import 'record.dart';

/// A CRDT backed by a in-memory map.
/// Useful for testing, or for applications which only require temporary datasets.
class MapCrdt<K, V> extends Crdt<K, V> {
  final _map = <K, Record<V>>{};
  final _controller = StreamController<MapEntry<K, V?>>.broadcast();

  @override
  final dynamic nodeId;

  MapCrdt._create(this.nodeId, [Map<K, Record<V>> seed = const {}]) {
    _map.addAll(seed);
  }

  static Future<MapCrdt<K, V>> create<K, V>(dynamic nodeId,
      [Map<K, Record<V>> seed = const {}]) async {
    final m = MapCrdt._create(nodeId, seed);
    await m.init();
    return m;
  }

  @override
  Future<bool> containsKey(K key) async => _map.containsKey(key);

  @override
  Future<Record<V>?> getRecord(K key) async => _map[key];

  @override
  Future<void> putRecord(K key, Record<V> value) async {
    _map[key] = value;
    _controller.add(MapEntry(key, value.value));
  }

  @override
  Future<void> putRecords(Map<K, Record<V>> recordMap) async {
    _map.addAll(recordMap);
    recordMap.map((key, value) => MapEntry(key, value.value)).entries.forEach(_controller.add);
  }

  @override
  Future<Map<K, Record<V>>> recordMap({Hlc? modifiedSince}) async => Map<K, Record<V>>.from(_map)
    ..removeWhere((_, record) => record.modified.logicalTime < (modifiedSince?.logicalTime ?? 0));

  @override
  Stream<MapEntry<K, V?>> watch({K? key}) =>
      _controller.stream.where((event) => key == null || key == event.key);

  @override
  Future<void> purge() async => _map.clear();
}
