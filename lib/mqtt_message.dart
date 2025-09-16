import 'dart:typed_data';
import 'dart:convert';

/// MQTT message types
enum MqttMessageType {
  connect(1),
  connack(2),
  publish(3),
  puback(4),
  pubrec(5),
  pubrel(6),
  pubcomp(7),
  subscribe(8),
  suback(9),
  unsubscribe(10),
  unsuback(11),
  pingreq(12),
  pingresp(13),
  disconnect(14);

  const MqttMessageType(this.value);
  final int value;

  static MqttMessageType? fromValue(int value) {
    for (final type in MqttMessageType.values) {
      if (type.value == value) return type;
    }
    return null;
  }
}

/// MQTT QoS levels
enum MqttQoS {
  atMostOnce(0),
  atLeastOnce(1),
  exactlyOnce(2),
  reserved(3);

  const MqttQoS(this.value);
  final int value;

  static MqttQoS fromValue(int value) {
    switch (value) {
      case 0:
        return MqttQoS.atMostOnce;
      case 1:
        return MqttQoS.atLeastOnce;
      case 2:
        return MqttQoS.exactlyOnce;
      case 3:
        return MqttQoS.reserved;
      default:
        return MqttQoS.atMostOnce;
    }
  }
}

/// MQTT message parsing result
class ParseResult {
  final int bytesConsumed;
  final dynamic value;

  ParseResult(this.bytesConsumed, this.value);
}

/// Base class for all MQTT messages
abstract class MqttMessage {
  final MqttMessageType messageType;
  final bool dup;
  final MqttQoS qos;
  final bool retain;

  MqttMessage({
    required this.messageType,
    this.dup = false,
    this.qos = MqttQoS.atMostOnce,
    this.retain = false,
  });

  /// Serialize the message to bytes
  Uint8List toBytes();

  /// Parse a message from bytes
  static MqttMessage? fromBytes(Uint8List data) {
    if (data.isEmpty) return null;

    final firstByte = data[0];
    final messageType = MqttMessageType.fromValue((firstByte >> 4) & 0x0F);
    if (messageType == null) return null;

    final dup = (firstByte & 0x08) != 0;
    final qos = MqttQoS.fromValue((firstByte >> 1) & 0x03);
    final retain = (firstByte & 0x01) != 0;

    switch (messageType) {
      case MqttMessageType.connect:
        return MqttConnectMessage.parse(data, dup, qos, retain);
      case MqttMessageType.publish:
        return MqttPublishMessage.parse(data, dup, qos, retain);
      case MqttMessageType.subscribe:
        return MqttSubscribeMessage.parse(data, dup, qos, retain);
      case MqttMessageType.unsubscribe:
        return MqttUnsubscribeMessage.parse(data, dup, qos, retain);
      case MqttMessageType.puback:
        return MqttPubackMessage.parse(data, dup, qos, retain);
      case MqttMessageType.pubrec:
        return MqttPubrecMessage.parse(data, dup, qos, retain);
      case MqttMessageType.pubrel:
        return MqttPubrelMessage.parse(data, dup, qos, retain);
      case MqttMessageType.pubcomp:
        return MqttPubcompMessage.parse(data, dup, qos, retain);
      case MqttMessageType.pingreq:
        return MqttPingreqMessage.parse(data, dup, qos, retain);
      case MqttMessageType.disconnect:
        return MqttDisconnectMessage.parse(data, dup, qos, retain);
      default:
        return null;
    }
  }

  /// Calculate remaining length
  static List<int> encodeRemainingLength(int length) {
    final bytes = <int>[];
    do {
      int digit = length % 128;
      length = length ~/ 128;
      if (length > 0) {
        digit |= 0x80;
      }
      bytes.add(digit);
    } while (length > 0);
    return bytes;
  }

  /// Decode remaining length from bytes - returns (length, bytesConsumed)
  static ParseResult decodeRemainingLength(Uint8List data, int offset) {
    int multiplier = 1;
    int value = 0;
    int index = offset;

    while (index < data.length) {
      final digit = data[index++];
      value += (digit & 127) * multiplier;
      if ((digit & 128) == 0) {
        return ParseResult(index - offset, value);
      }
      multiplier *= 128;
      if (multiplier > 128 * 128 * 128) {
        throw Exception('Malformed remaining length');
      }
    }
    throw Exception('Incomplete remaining length');
  }

  /// Encode UTF-8 string with length prefix
  static List<int> encodeString(String str) {
    final utf8Bytes = utf8.encode(str);
    final length = utf8Bytes.length;
    if (length > 65535) {
      throw Exception('String too long for MQTT');
    }
    return [
      (length >> 8) & 0xFF,
      length & 0xFF,
      ...utf8Bytes,
    ];
  }

  /// Decode UTF-8 string from bytes - returns (string, bytesConsumed)
  static ParseResult decodeString(Uint8List data, int offset) {
    if (offset + 2 > data.length) {
      throw Exception('Insufficient data for string length');
    }

    final length = (data[offset] << 8) | data[offset + 1];
    if (offset + 2 + length > data.length) {
      throw Exception('Insufficient data for string content');
    }

    final utf8Bytes = data.sublist(offset + 2, offset + 2 + length);
    final str = utf8.decode(utf8Bytes);
    return ParseResult(2 + length, str);
  }

  /// Validate topic name (for publishing)
  static bool isValidTopicName(String topic) {
    if (topic.isEmpty) return false;
    if (topic.contains('+') || topic.contains('#')) return false; // Wildcards not allowed in publish
    if (topic.startsWith('\$')) return false; // System topics not allowed for normal publish
    // Check for null characters and other invalid UTF-8
    if (topic.contains('\u0000')) return false;
    return true;
  }

  /// Validate topic filter (for subscriptions)
  static bool isValidTopicFilter(String filter) {
    if (filter.isEmpty) return false;

    // Check for null characters
    if (filter.contains('\u0000')) return false;

    // System topics ($SYS/*) are allowed for subscription but not for publishing
    // This is correct behavior according to MQTT spec

    // Check wildcard placement
    final parts = filter.split('/');
    for (int i = 0; i < parts.length; i++) {
      final part = parts[i];
      if (part == '#') {
        // # must be last level and alone
        if (i != parts.length - 1) return false;
      } else if (part.contains('#')) {
        // # must be alone in its level
        return false;
      } else if (part.contains('+') && part != '+') {
        // + must be alone in its level
        return false;
      }
    }
    return true;
  }
}

/// MQTT CONNECT message
class MqttConnectMessage extends MqttMessage {
  final String clientId;
  final bool cleanSession;
  final int keepAlive;
  final String? username;
  final String? password;
  final String? willTopic;
  final String? willMessage;
  final MqttQoS willQoS;
  final bool willRetain;

  MqttConnectMessage({
    required this.clientId,
    this.cleanSession = true,
    this.keepAlive = 60,
    this.username,
    this.password,
    this.willTopic,
    this.willMessage,
    this.willQoS = MqttQoS.atMostOnce,
    this.willRetain = false,
  }) : super(messageType: MqttMessageType.connect);

  @override
  Uint8List toBytes() {
    final payload = <int>[];

    // Protocol name
    payload.addAll(MqttMessage.encodeString('MQTT'));

    // Protocol level
    payload.add(4);

    // Connect flags
    int flags = 0;
    if (cleanSession) flags |= 0x02;
    if (willTopic != null) {
      flags |= 0x04;
      flags |= (willQoS.value << 3);
      if (willRetain) flags |= 0x20;
    }
    if (password != null) flags |= 0x40;
    if (username != null) flags |= 0x80;
    payload.add(flags);

    // Keep alive
    payload.add((keepAlive >> 8) & 0xFF);
    payload.add(keepAlive & 0xFF);

    // Client ID
    payload.addAll(MqttMessage.encodeString(clientId));

    // Will topic and message
    if (willTopic != null) {
      payload.addAll(MqttMessage.encodeString(willTopic!));
      payload.addAll(MqttMessage.encodeString(willMessage ?? ''));
    }

    // Username and password
    if (username != null) {
      payload.addAll(MqttMessage.encodeString(username!));
    }
    if (password != null) {
      payload.addAll(MqttMessage.encodeString(password!));
    }

    final remainingLength = MqttMessage.encodeRemainingLength(payload.length);
    final result = <int>[];
    result.add(0x10); // CONNECT message type
    result.addAll(remainingLength);
    result.addAll(payload);

    return Uint8List.fromList(result);
  }

  static MqttConnectMessage? parse(Uint8List data, bool dup, MqttQoS qos, bool retain) {
    if (data.length < 10) return null;

    try {
      int offset = 1;
      final lengthResult = MqttMessage.decodeRemainingLength(data, offset);
      offset += lengthResult.bytesConsumed;
      final remainingLength = lengthResult.value as int;

      final expectedEnd = offset + remainingLength;
      if (data.length < expectedEnd) return null;

      // Protocol name
      final protocolResult = MqttMessage.decodeString(data, offset);
      offset += protocolResult.bytesConsumed;
      final protocolName = protocolResult.value as String;

      if (protocolName != 'MQTT') return null;

      // Protocol level
      if (offset >= data.length) return null;
      final protocolLevel = data[offset++];
      if (protocolLevel != 4) return null; // Only support MQTT 3.1.1

      // Connect flags
      if (offset >= data.length) return null;
      final flags = data[offset++];
      final cleanSession = (flags & 0x02) != 0;
      final willFlag = (flags & 0x04) != 0;
      final willQoS = MqttQoS.fromValue((flags >> 3) & 0x03);
      final willRetain = (flags & 0x20) != 0;
      final passwordFlag = (flags & 0x40) != 0;
      final usernameFlag = (flags & 0x80) != 0;

      // Keep alive
      if (offset + 2 > data.length) return null;
      final keepAlive = (data[offset] << 8) | data[offset + 1];
      offset += 2;

      // Client ID
      final clientIdResult = MqttMessage.decodeString(data, offset);
      offset += clientIdResult.bytesConsumed;
      final clientId = clientIdResult.value as String;

      String? willTopic;
      String? willMessage;
      String? username;
      String? password;

      // Will topic and message
      if (willFlag) {
        final willTopicResult = MqttMessage.decodeString(data, offset);
        offset += willTopicResult.bytesConsumed;
        willTopic = willTopicResult.value as String;

        final willMessageResult = MqttMessage.decodeString(data, offset);
        offset += willMessageResult.bytesConsumed;
        willMessage = willMessageResult.value as String;
      }

      // Username
      if (usernameFlag) {
        final usernameResult = MqttMessage.decodeString(data, offset);
        offset += usernameResult.bytesConsumed;
        username = usernameResult.value as String;
      }

      // Password
      if (passwordFlag) {
        final passwordResult = MqttMessage.decodeString(data, offset);
        offset += passwordResult.bytesConsumed;
        password = passwordResult.value as String;
      }

      return MqttConnectMessage(
        clientId: clientId,
        cleanSession: cleanSession,
        keepAlive: keepAlive,
        username: username,
        password: password,
        willTopic: willTopic,
        willMessage: willMessage,
        willQoS: willQoS,
        willRetain: willRetain,
      );
    } catch (e) {
      return null;
    }
  }
}

/// MQTT CONNACK message
class MqttConnackMessage extends MqttMessage {
  final bool sessionPresent;
  final int returnCode;

  MqttConnackMessage({
    this.sessionPresent = false,
    required this.returnCode,
  }) : super(messageType: MqttMessageType.connack);

  @override
  Uint8List toBytes() {
    final payload = <int>[];
    payload.add(sessionPresent ? 0x01 : 0x00);
    payload.add(returnCode);

    return Uint8List.fromList([
      0x20, // CONNACK message type
      0x02, // Remaining length
      ...payload,
    ]);
  }
}

/// MQTT PUBLISH message
class MqttPublishMessage extends MqttMessage {
  final String topic;
  final int? packetId;
  final Uint8List payload;

  MqttPublishMessage({
    required this.topic,
    this.packetId,
    required this.payload,
    super.dup = false,
    super.qos = MqttQoS.atMostOnce,
    super.retain = false,
  }) : super(messageType: MqttMessageType.publish);

  @override
  Uint8List toBytes() {
    final data = <int>[];

    // Topic
    data.addAll(MqttMessage.encodeString(topic));

    // Packet ID (only for QoS 1 and 2)
    if (qos != MqttQoS.atMostOnce && packetId != null) {
      data.add((packetId! >> 8) & 0xFF);
      data.add(packetId! & 0xFF);
    }

    // Payload
    data.addAll(payload);

    final remainingLength = MqttMessage.encodeRemainingLength(data.length);

    // Fixed header
    int firstByte = 0x30; // PUBLISH base
    if (dup) firstByte |= 0x08;
    firstByte |= (qos.value << 1);
    if (retain) firstByte |= 0x01;

    final result = <int>[];
    result.add(firstByte);
    result.addAll(remainingLength);
    result.addAll(data);

    return Uint8List.fromList(result);
  }

  static MqttPublishMessage? parse(Uint8List data, bool dup, MqttQoS qos, bool retain) {
    if (data.length < 4) return null;

    try {
      int offset = 1;
      final lengthResult = MqttMessage.decodeRemainingLength(data, offset);
      offset += lengthResult.bytesConsumed;

      // Topic
      final topicResult = MqttMessage.decodeString(data, offset);
      offset += topicResult.bytesConsumed;
      final topic = topicResult.value as String;

      int? packetId;
      if (qos != MqttQoS.atMostOnce) {
        if (offset + 2 > data.length) return null;
        packetId = (data[offset] << 8) | data[offset + 1];
        offset += 2;
      }

      final payload = data.sublist(offset);

      return MqttPublishMessage(
        topic: topic,
        packetId: packetId,
        payload: payload,
        dup: dup,
        qos: qos,
        retain: retain,
      );
    } catch (e) {
      return null;
    }
  }
}

/// MQTT SUBSCRIBE message
class MqttSubscribeMessage extends MqttMessage {
  final int packetId;
  final List<String> topics;
  final List<MqttQoS> qosLevels;

  MqttSubscribeMessage({
    required this.packetId,
    required this.topics,
    required this.qosLevels,
  }) : super(messageType: MqttMessageType.subscribe, qos: MqttQoS.atLeastOnce);

  @override
  Uint8List toBytes() {
    final payload = <int>[];

    // Packet ID
    payload.add((packetId >> 8) & 0xFF);
    payload.add(packetId & 0xFF);

    // Topics and QoS
    for (int i = 0; i < topics.length; i++) {
      payload.addAll(MqttMessage.encodeString(topics[i]));
      payload.add(qosLevels[i].value);
    }

    final remainingLength = MqttMessage.encodeRemainingLength(payload.length);
    final result = <int>[];
    result.add(0x82); // SUBSCRIBE message type with flags
    result.addAll(remainingLength);
    result.addAll(payload);

    return Uint8List.fromList(result);
  }

  static MqttSubscribeMessage? parse(Uint8List data, bool dup, MqttQoS qos, bool retain) {
    if (data.length < 5) return null;

    try {
      int offset = 1;
      final lengthResult = MqttMessage.decodeRemainingLength(data, offset);
      offset += lengthResult.bytesConsumed;
      final remainingLength = lengthResult.value as int;
      final endOffset = offset + remainingLength;

      if (data.length < endOffset) return null;

      // Packet ID
      if (offset + 2 > endOffset) return null;
      final packetId = (data[offset] << 8) | data[offset + 1];
      offset += 2;

      final topics = <String>[];
      final qosLevels = <MqttQoS>[];

      while (offset < endOffset) {
        final topicResult = MqttMessage.decodeString(data, offset);
        offset += topicResult.bytesConsumed;
        final topic = topicResult.value as String;

        if (offset >= endOffset) return null;
        final qosLevel = MqttQoS.fromValue(data[offset++]);

        topics.add(topic);
        qosLevels.add(qosLevel);
      }

      return MqttSubscribeMessage(
        packetId: packetId,
        topics: topics,
        qosLevels: qosLevels,
      );
    } catch (e) {
      return null;
    }
  }
}

/// MQTT SUBACK message
class MqttSubackMessage extends MqttMessage {
  final int packetId;
  final List<int> returnCodes;

  MqttSubackMessage({
    required this.packetId,
    required this.returnCodes,
  }) : super(messageType: MqttMessageType.suback);

  @override
  Uint8List toBytes() {
    final payload = <int>[];
    payload.add((packetId >> 8) & 0xFF);
    payload.add(packetId & 0xFF);
    payload.addAll(returnCodes);

    final remainingLength = MqttMessage.encodeRemainingLength(payload.length);
    final result = <int>[];
    result.add(0x90); // SUBACK message type
    result.addAll(remainingLength);
    result.addAll(payload);

    return Uint8List.fromList(result);
  }
}

/// MQTT UNSUBSCRIBE message
class MqttUnsubscribeMessage extends MqttMessage {
  final int packetId;
  final List<String> topics;

  MqttUnsubscribeMessage({
    required this.packetId,
    required this.topics,
  }) : super(messageType: MqttMessageType.unsubscribe, qos: MqttQoS.atLeastOnce);

  @override
  Uint8List toBytes() {
    final payload = <int>[];

    // Packet ID
    payload.add((packetId >> 8) & 0xFF);
    payload.add(packetId & 0xFF);

    // Topics
    for (final topic in topics) {
      payload.addAll(MqttMessage.encodeString(topic));
    }

    final remainingLength = MqttMessage.encodeRemainingLength(payload.length);
    final result = <int>[];
    result.add(0xA2); // UNSUBSCRIBE message type with flags
    result.addAll(remainingLength);
    result.addAll(payload);

    return Uint8List.fromList(result);
  }

  static MqttUnsubscribeMessage? parse(Uint8List data, bool dup, MqttQoS qos, bool retain) {
    if (data.length < 5) return null;

    try {
      int offset = 1;
      final lengthResult = MqttMessage.decodeRemainingLength(data, offset);
      offset += lengthResult.bytesConsumed;
      final remainingLength = lengthResult.value as int;
      final endOffset = offset + remainingLength;

      if (data.length < endOffset) return null;

      // Packet ID
      if (offset + 2 > endOffset) return null;
      final packetId = (data[offset] << 8) | data[offset + 1];
      offset += 2;

      final topics = <String>[];

      while (offset < endOffset) {
        final topicResult = MqttMessage.decodeString(data, offset);
        offset += topicResult.bytesConsumed;
        topics.add(topicResult.value as String);
      }

      return MqttUnsubscribeMessage(
        packetId: packetId,
        topics: topics,
      );
    } catch (e) {
      return null;
    }
  }
}

/// MQTT UNSUBACK message
class MqttUnsubackMessage extends MqttMessage {
  final int packetId;

  MqttUnsubackMessage({
    required this.packetId,
  }) : super(messageType: MqttMessageType.unsuback);

  @override
  Uint8List toBytes() {
    return Uint8List.fromList([
      0xB0, // UNSUBACK message type
      0x02, // Remaining length
      (packetId >> 8) & 0xFF,
      packetId & 0xFF,
    ]);
  }
}

/// MQTT PUBACK message
class MqttPubackMessage extends MqttMessage {
  final int packetId;

  MqttPubackMessage({
    required this.packetId,
  }) : super(messageType: MqttMessageType.puback);

  @override
  Uint8List toBytes() {
    return Uint8List.fromList([
      0x40, // PUBACK message type
      0x02, // Remaining length
      (packetId >> 8) & 0xFF,
      packetId & 0xFF,
    ]);
  }

  static MqttPubackMessage? parse(Uint8List data, bool dup, MqttQoS qos, bool retain) {
    if (data.length < 4) return null;
    int offset = 2; // Skip message type and remaining length
    final packetId = (data[offset] << 8) | data[offset + 1];
    return MqttPubackMessage(packetId: packetId);
  }
}

/// MQTT PUBREC message
class MqttPubrecMessage extends MqttMessage {
  final int packetId;

  MqttPubrecMessage({
    required this.packetId,
  }) : super(messageType: MqttMessageType.pubrec);

  @override
  Uint8List toBytes() {
    return Uint8List.fromList([
      0x50, // PUBREC message type
      0x02, // Remaining length
      (packetId >> 8) & 0xFF,
      packetId & 0xFF,
    ]);
  }

  static MqttPubrecMessage? parse(Uint8List data, bool dup, MqttQoS qos, bool retain) {
    if (data.length < 4) return null;
    int offset = 2; // Skip message type and remaining length
    final packetId = (data[offset] << 8) | data[offset + 1];
    return MqttPubrecMessage(packetId: packetId);
  }
}

/// MQTT PUBREL message
class MqttPubrelMessage extends MqttMessage {
  final int packetId;

  MqttPubrelMessage({
    required this.packetId,
  }) : super(messageType: MqttMessageType.pubrel, qos: MqttQoS.atLeastOnce);

  @override
  Uint8List toBytes() {
    return Uint8List.fromList([
      0x62, // PUBREL message type with flags
      0x02, // Remaining length
      (packetId >> 8) & 0xFF,
      packetId & 0xFF,
    ]);
  }

  static MqttPubrelMessage? parse(Uint8List data, bool dup, MqttQoS qos, bool retain) {
    if (data.length < 4) return null;
    int offset = 2; // Skip message type and remaining length
    final packetId = (data[offset] << 8) | data[offset + 1];
    return MqttPubrelMessage(packetId: packetId);
  }
}

/// MQTT PUBCOMP message
class MqttPubcompMessage extends MqttMessage {
  final int packetId;

  MqttPubcompMessage({
    required this.packetId,
  }) : super(messageType: MqttMessageType.pubcomp);

  @override
  Uint8List toBytes() {
    return Uint8List.fromList([
      0x70, // PUBCOMP message type
      0x02, // Remaining length
      (packetId >> 8) & 0xFF,
      packetId & 0xFF,
    ]);
  }

  static MqttPubcompMessage? parse(Uint8List data, bool dup, MqttQoS qos, bool retain) {
    if (data.length < 4) return null;
    int offset = 2; // Skip message type and remaining length
    final packetId = (data[offset] << 8) | data[offset + 1];
    return MqttPubcompMessage(packetId: packetId);
  }
}

/// MQTT PINGREQ message
class MqttPingreqMessage extends MqttMessage {
  MqttPingreqMessage() : super(messageType: MqttMessageType.pingreq);

  @override
  Uint8List toBytes() {
    return Uint8List.fromList([0xC0, 0x00]); // PINGREQ
  }

  static MqttPingreqMessage? parse(Uint8List data, bool dup, MqttQoS qos, bool retain) {
    return MqttPingreqMessage();
  }
}

/// MQTT PINGRESP message
class MqttPingrespMessage extends MqttMessage {
  MqttPingrespMessage() : super(messageType: MqttMessageType.pingresp);

  @override
  Uint8List toBytes() {
    return Uint8List.fromList([0xD0, 0x00]); // PINGRESP
  }
}

/// MQTT DISCONNECT message
class MqttDisconnectMessage extends MqttMessage {
  MqttDisconnectMessage() : super(messageType: MqttMessageType.disconnect);

  @override
  Uint8List toBytes() {
    return Uint8List.fromList([0xE0, 0x00]); // DISCONNECT
  }

  static MqttDisconnectMessage? parse(Uint8List data, bool dup, MqttQoS qos, bool retain) {
    return MqttDisconnectMessage();
  }
}
