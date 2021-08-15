import 'dart:io';

import 'package:crdt/crdt.dart';
import 'package:test/test.dart';

import 'crdt_test.dart';

const _millis = 1000000000000;
const _isoTime = '2001-09-09T01:46:40.000Z';

void main() {
  final hlcNow = Hlc.now('abc');

  crdtTests<MapCrdt<String, int>>('abc', asyncSetup: () async => await MapCrdt.create('abc'));

  group('Seed', () {
    late Crdt<String, int> crdt;

    setUp(() async {
      crdt = await MapCrdt.create<String, int>('abc', {'x': Record(hlcNow, 1, hlcNow)});
    });

    test('Seed item', () async {
      expect((await crdt.get('x')), 1);
    });

    test('Seed and put', () async {
      await crdt.put('x', 2);
      expect((await crdt.get('x')), 2);
    });
  });

  group('Merge', () {
    late Crdt<String, int> crdt;

    setUp(() async {
      crdt = await MapCrdt.create<String, int>('abc');
    });

    test('Merge older', () async {
      await crdt.put('x', 2);
      await crdt.merge({'x': Record(Hlc(_millis - 1, 0, 'xyz'), 1, hlcNow)});
      expect(await crdt.get('x'), 2);
    });

    test('Merge very old', () async {
      await crdt.put('x', 2);
      await crdt.merge({'x': Record(Hlc(0, 0, 'xyz'), 1, hlcNow)});
      expect(await crdt.get('x'), 2);
    });

    test('Merge newer', () async {
      await crdt.put('x', 1);
      await Future.delayed(Duration(milliseconds: 1));
      await crdt.merge({'x': Record(Hlc.now('xyz'), 2, hlcNow)});
      expect(await crdt.get('x'), 2);
    });

    test('Disambiguate using node id', () async {
      await crdt.merge({'x': Record(Hlc(_millis, 0, 'nodeA'), 1, hlcNow)});
      await crdt.merge({'x': Record(Hlc(_millis, 0, 'nodeB'), 2, hlcNow)});
      expect(await crdt.get('x'), 2);
    });

    test('Merge same', () async {
      await crdt.put('x', 2);
      final remoteTs = (await crdt.getRecord('x'))!.hlc;
      await crdt.merge({'x': Record(remoteTs, 1, hlcNow)});
      expect(await crdt.get('x'), 2);
    });

    test('Merge older, newer counter', () async {
      await crdt.put('x', 2);
      await crdt.merge({'x': Record(Hlc(_millis - 1, 2, 'xyz'), 1, hlcNow)});
      expect(await crdt.get('x'), 2);
    });

    test('Merge same, newer counter', () async {
      await crdt.put('x', 1);
      final remoteTs = Hlc((await crdt.getRecord('x'))!.hlc.millis, 2, 'xyz');
      await crdt.merge({'x': Record(remoteTs, 2, hlcNow)});
      expect(await crdt.get('x'), 2);
    });

    test('Merge new item', () async {
      final map = {'x': Record<int>(Hlc.now('xyz'), 2, hlcNow)};
      await crdt.merge(map);
      expect(await crdt.recordMap(), map);
    });

    test('Merge deleted item', () async {
      await crdt.put('x', 1);
      await Future.delayed(Duration(milliseconds: 1));
      await crdt.merge({'x': Record(Hlc.now('xyz'), null, hlcNow)});
      expect(await crdt.isDeleted('x'), isTrue);
    });

    test('Update HLC on merge', () async {
      await crdt.put('x', 1);
      await crdt.merge({'y': Record(Hlc(_millis - 1, 0, 'xyz'), 2, hlcNow)});
      expect(await crdt.values, [1, 2]);
    });
  });

  group('Serialization', () {
    test('To map', () async {
      final crdt = await MapCrdt.create('abc', {
        'x': Record<int>(Hlc(_millis, 0, 'abc'), 1, hlcNow),
      });
      expect(await crdt.recordMap(), {'x': Record<int>(Hlc(_millis, 0, 'abc'), 1, hlcNow)});
    });

    test('jsonEncodeStringKey', () async {
      final crdt = await MapCrdt.create<String, int>('abc', {
        'x': Record(Hlc(_millis, 0, 'abc'), 1, hlcNow),
      });
      expect(await crdt.toJson(), '{"x":{"hlc":"$_isoTime-0000-abc","value":1}}');
    });

    test('jsonEncodeIntKey', () async {
      final crdt = await MapCrdt.create<int, int>('abc', {
        1: Record(Hlc(_millis, 0, 'abc'), 1, hlcNow),
      });
      expect(await crdt.toJson(), '{"1":{"hlc":"$_isoTime-0000-abc","value":1}}');
    });

    test('jsonEncodeDateTimeKey', () async {
      final crdt = await MapCrdt.create<DateTime, int>('abc', {
        DateTime(2000, 01, 01, 01, 20): Record(Hlc(_millis, 0, 'abc'), 1, hlcNow),
      });
      expect(await crdt.toJson(),
          '{"2000-01-01 01:20:00.000":{"hlc":"$_isoTime-0000-abc","value":1}}');
    });

    test('jsonEncodeCustomClassValue', () async {
      final crdt = await MapCrdt.create<String, TestClass>('abc', {
        'x': Record(Hlc(_millis, 0, 'abc'), TestClass('test'), hlcNow),
      });
      expect(await crdt.toJson(), '{"x":{"hlc":"$_isoTime-0000-abc","value":{"test":"test"}}}');
    });

    test('jsonEncodeCustomNodeId', () async {
      final crdt = await MapCrdt.create<String, int>('abc', {
        'x': Record(Hlc<int>(_millis, 0, 1), 0, hlcNow),
      });
      expect(await crdt.toJson(), '{"x":{"hlc":"$_isoTime-0000-1","value":0}}');
    });

    test('jsonDecodeStringKey', () async {
      final crdt = await MapCrdt.create<String, int>('abc');
      final map =
          CrdtJson.decode<String, int>('{"x":{"hlc":"$_isoTime-0000-abc","value":1}}', hlcNow);
      await crdt.putRecords(map);
      expect(await crdt.recordMap(), {'x': Record<int>(Hlc(_millis, 0, 'abc'), 1, hlcNow)});
    });

    test('jsonDecodeIntKey', () async {
      final crdt = await MapCrdt.create<int, int>('abc');
      final map = CrdtJson.decode<int, int>('{"1":{"hlc":"$_isoTime-0000-abc","value":1}}', hlcNow,
          keyDecoder: (key) => int.parse(key));
      await crdt.putRecords(map);
      expect(await crdt.recordMap(), {1: Record(Hlc(_millis, 0, 'abc'), 1, hlcNow)});
    });

    test('jsonDecodeDateTimeKey', () async {
      final crdt = await MapCrdt.create<DateTime, int>('abc');
      final map = CrdtJson.decode<DateTime, int>(
          '{"2000-01-01 01:20:00.000":{"hlc":"$_isoTime-0000-abc","value":1}}', hlcNow,
          keyDecoder: (key) => DateTime.parse(key));
      await crdt.putRecords(map);
      expect(await crdt.recordMap(),
          {DateTime(2000, 01, 01, 01, 20): Record(Hlc(_millis, 0, 'abc'), 1, hlcNow)});
    });

    test('jsonDecodeCustomClassValue', () async {
      final crdt = await MapCrdt.create<String, TestClass>('abc');
      final map = CrdtJson.decode<String, TestClass>(
          '{"x":{"hlc":"$_isoTime-0000-abc","value":{"test":"test"}}}', hlcNow,
          valueDecoder: (key, value) => TestClass.fromJson(value));
      await crdt.putRecords(map);
      expect(
          await crdt.recordMap(), {'x': Record(Hlc(_millis, 0, 'abc'), TestClass('test'), hlcNow)});
    });

    test('jsonDecodeCustomNodeId', () async {
      final crdt = await MapCrdt.create<String, int>('abc');
      final map = CrdtJson.decode<String, int>('{"x":{"hlc":"$_isoTime-0000-1","value":0}}', hlcNow,
          nodeIdDecoder: int.parse);
      await crdt.putRecords(map);
      expect(await crdt.recordMap(), {'x': Record(Hlc(_millis, 0, 1), 0, hlcNow)});
    });
  });

  group('Delta subsets', () {
    late Crdt<String, int> crdt;
    final hlc1 = Hlc(_millis, 0, 'abc');
    final hlc2 = Hlc(_millis + 1, 0, 'abc');
    final hlc3 = Hlc(_millis + 2, 0, 'abc');

    setUp(() async {
      crdt = await MapCrdt.create('abc', {
        'x': Record(hlc1, 1, hlc1),
        'y': Record(hlc2, 2, hlc2),
      });
    });

    test('null modifiedSince', () async {
      final map = await crdt.recordMap();
      expect(map.length, 2);
    });

    test('modifiedSince hlc1', () async {
      final map = await crdt.recordMap(modifiedSince: hlc1);
      expect(map.length, 2);
    });

    test('modifiedSince hlc2', () async {
      final map = await crdt.recordMap(modifiedSince: hlc2);
      expect(map.length, 1);
    });

    test('modifiedSince hlc3', () async {
      final map = await crdt.recordMap(modifiedSince: hlc3);
      expect(map.length, 0);
    });
  });

  group('Delta sync', () {
    late Crdt<String, int> crdtA;
    late Crdt<String, int> crdtB;
    late Crdt<String, int> crdtC;

    setUp(() async {
      crdtA = await MapCrdt.create('a');
      crdtB = await MapCrdt.create('b');
      crdtC = await MapCrdt.create('c');

      await crdtA.put('x', 1);
      sleep(Duration(milliseconds: 100));
      await crdtB.put('x', 2);
    });

    test('Merge in order', () async {
      await _sync(crdtA, crdtC);
      await _sync(crdtB, crdtC);

      expect(await crdtA.get('x'), 1); // node A still contains the old value
      expect(await crdtB.get('x'), 2);
      expect(await crdtC.get('x'), 2);
    });

    test('Merge in reverse order', () async {
      await _sync(crdtB, crdtC);
      await _sync(crdtA, crdtC);
      await _sync(crdtB, crdtC);

      expect(await crdtA.get('x'), 2);
      expect(await crdtB.get('x'), 2);
      expect(await crdtC.get('x'), 2);
    });
  });
}

Future<void> _sync(Crdt local, Crdt remote) async {
  final time = local.canonicalTime;
  final l = await local.recordMap();
  await remote.merge(l);
  final r = await remote.recordMap(modifiedSince: time);
  await local.merge(r);
}

class TestClass {
  final String test;

  TestClass(this.test);

  static TestClass fromJson(dynamic map) => TestClass(map['test']);

  Map<String, dynamic> toJson() => {'test': test};

  @override
  bool operator ==(other) => other is TestClass && test == other.test;

  @override
  String toString() => test;
}
