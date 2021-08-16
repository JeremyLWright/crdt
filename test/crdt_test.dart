import 'package:crdt/crdt.dart';
import 'package:test/test.dart';

// Make dart test happy
void main() {}

void crdtTests<T extends Crdt<String, int>>(String nodeId,
    {T Function()? syncSetup,
    Future<T> Function()? asyncSetup,
    void Function(T crdt)? syncTearDown,
    Future<void> Function(T crdt)? asyncTearDown}) {
  group('Basic', () {
    late T crdt;

    setUp(() async {
      crdt = syncSetup != null ? syncSetup() : await asyncSetup!();
    });

    test('Node ID', () async {
      expect(crdt.nodeId, nodeId);
    });

    test('Empty', () async {
      expect(await crdt.isEmpty, isTrue);
      expect(await crdt.length, 0);
      expect(await crdt.map, {});
      expect(await crdt.keys, []);
      expect(await crdt.values, []);
    });

    test('One record', () async {
      await crdt.put('x', 1);

      expect(await crdt.isEmpty, isFalse);
      expect(await crdt.length, 1);
      expect(await crdt.map, {'x': 1});
      expect(await crdt.keys, ['x']);
      expect(await crdt.values, [1]);
    });

    test('Empty after deleted record', () async {
      await crdt.put('x', 1);
      await crdt.delete('x');

      expect(await crdt.isEmpty, isTrue);
      expect(await crdt.length, 0);
      expect(await crdt.map, {});
      expect(await crdt.keys, []);
      expect(await crdt.values, []);
    });

    test('Put', () async {
      await crdt.put('x', 1);
      expect(await crdt.get('x'), 1);
    });

    test('Update existing', () async {
      await crdt.put('x', 1);
      await crdt.put('x', 2);
      expect(await crdt.get('x'), 2);
    });

    test('Put many', () async {
      await crdt.putAll({'x': 2, 'y': 3});
      expect(await crdt.get('x'), 2);
      expect(await crdt.get('y'), 3);
    });

    test('Delete value', () async {
      await crdt.put('x', 1);
      await crdt.put('y', 2);
      await crdt.delete('x');
      expect(await crdt.isDeleted('x'), isTrue);
      expect(await crdt.isDeleted('y'), isFalse);
      expect(await crdt.get('x'), null);
      expect(await crdt.get('y'), 2);
    });

    test('Clear', () async {
      await crdt.put('x', 1);
      await crdt.put('y', 2);
      await crdt.clear();
      expect(await crdt.isDeleted('x'), isTrue);
      expect(await crdt.isDeleted('y'), isTrue);
      expect(await crdt.get('x'), null);
      expect(await crdt.get('y'), null);
    });

    tearDown(() async {
      if (syncTearDown != null) syncTearDown(crdt);
      if (asyncTearDown != null) await asyncTearDown(crdt);
    });
  });

  group('Watch', () {
    late T crdt;

    setUp(() async {
      crdt = syncSetup != null ? syncSetup() : await asyncSetup!();
    });

    test('All changes', () async {
      final streamTest = expectLater(
          crdt.watch(),
          emitsInAnyOrder([
            (MapEntry<String, int?> event) => event.key == 'x' && event.value == 1,
            (MapEntry<String, int?> event) => event.key == 'y' && event.value == 2,
          ]));
      await crdt.put('x', 1);
      await crdt.put('y', 2);
      await streamTest;
    });

    test('Key', () async {
      final streamTest = expectLater(
          crdt.watch(key: 'y'),
          emits(
            (event) => event.key == 'y' && event.value == 2,
          ));
      await crdt.put('x', 1);
      await crdt.put('y', 2);
      await streamTest;
    });

    tearDown(() async {
      if (syncTearDown != null) syncTearDown(crdt);
      if (asyncTearDown != null) await asyncTearDown(crdt);
    });
  });
}
