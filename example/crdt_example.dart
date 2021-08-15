import 'package:crdt/crdt.dart';

void main() async {
  var crdt = await MapCrdt.create('node_id');

  // Insert a record
  await crdt.put('a', 1);
  // Read the record
  print('Record: ${await crdt.get('a')}');

  // Export the CRDT as Json
  final json = await crdt.toJson();
  print('json records $json');
  // Send to remote node
  final remoteJson = sendToRemote(json);
  // Merge remote CRDT with local
  crdt.mergeJson(remoteJson);
  // Verify updated record
  print('Record after merging: ${await crdt.get('a')}');
}

// Mock sending the CRDT to a remote node and getting an updated one back
String sendToRemote(String json) {
  final hlc = Hlc.now('another_nodeId');
  return '{"a":{"hlc":"$hlc","value":2}}';
}
