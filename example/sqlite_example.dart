import 'package:crdt/todo/database.dart';
import 'package:crdt/todo/todo_crdt.dart';

final String nodeA = 'nodeA';
final String nodeB = 'nodeB';

void main() async {
  final todoRepo = await TodoCrdt.create(nodeA);

  // Insert a record
  await todoRepo.put(
      1,
      TodoDto(
        title: 'Implement CRDT',
        content: 'sync All the things',
      ));
  // Read the record
  print('Record: ${await todoRepo.get(1)}');

  // Export the CRDT as Json
  final records = await todoRepo.toJson(keyEncoder: (k) => '$k');
  print('json records $records');
  // Send to remote node
  final remoteRecords = await sendToRemote(records);
  //// Merge remote CRDT with local
  await todoRepo.mergeJson(remoteRecords,
      keyDecoder: (s) => int.parse(s), valueDecoder: (k, s) => TodoDto.fromJson(s));

  //// Verify updated record
  print('Record after merging: ${await todoRepo.get(1)}');
  print('json records ${await todoRepo.toJson()}');
}

// Mock sending the CRDT to a remote node and getting an updated one back
Future<String> sendToRemote(String fromClient) async {
  //final hlc = Hlc.now(nodeB);
  final bRepo = await TodoCrdt.create(nodeB);
  await bRepo.mergeJson(fromClient,
      keyDecoder: (s) => int.parse(s), valueDecoder: (k, s) => TodoDto.fromJson(s));
  final n = await bRepo.get(2);
  if (n != null) {
    n.title = n.title + 'Look it\'s different!';
    await bRepo.put(2, n);
  } else {
    await bRepo.put(2, TodoDto(title: 'From Node b', content: 'thing'));
  }

  return bRepo.toJson();
}
