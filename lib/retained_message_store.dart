import 'mqtt_message.dart';

/// Storage for retained messages
class RetainedMessageStore {
  /// Map of topic -> retained message
  final Map<String, MqttPublishMessage> _retainedMessages = {};

  /// Store a retained message
  void storeMessage(String topic, MqttPublishMessage message) {
    if (message.retain) {
      if (message.payload.isEmpty) {
        // Empty payload means delete the retained message
        _retainedMessages.remove(topic);
      } else {
        // Create a copy of the message for storage
        final retainedMessage = MqttPublishMessage(
          topic: topic,
          packetId: null, // Retained messages don't have packet IDs
          payload: message.payload,
          qos: message.qos,
          retain: true,
        );
        _retainedMessages[topic] = retainedMessage;
      }
    }
  }

  /// Get all retained messages that match a topic filter
  List<MqttPublishMessage> getMatchingMessages(String topicFilter) {
    final matchingMessages = <MqttPublishMessage>[];

    for (final entry in _retainedMessages.entries) {
      final topic = entry.key;
      final message = entry.value;

      if (_matchesTopic(topicFilter, topic)) {
        matchingMessages.add(message);
      }
    }

    return matchingMessages;
  }

  /// Get a specific retained message by topic
  MqttPublishMessage? getMessage(String topic) {
    return _retainedMessages[topic];
  }

  /// Remove a retained message
  void removeMessage(String topic) {
    _retainedMessages.remove(topic);
  }

  /// Clear all retained messages
  void clear() {
    _retainedMessages.clear();
  }

  /// Get all retained topics
  Set<String> getAllTopics() {
    return _retainedMessages.keys.toSet();
  }

  /// Get total number of retained messages
  int get count => _retainedMessages.length;

  /// Check if topic matches a subscription pattern
  bool _matchesTopic(String pattern, String topic) {
    // Handle single-level wildcard (+)
    if (pattern.contains('+')) {
      final patternParts = pattern.split('/');
      final topicParts = topic.split('/');

      if (patternParts.length != topicParts.length) {
        return false;
      }

      for (int i = 0; i < patternParts.length; i++) {
        if (patternParts[i] != '+' && patternParts[i] != topicParts[i]) {
          return false;
        }
      }
      return true;
    }

    // Handle multi-level wildcard (#)
    if (pattern.endsWith('/#')) {
      final prefix = pattern.substring(0, pattern.length - 2);
      return topic.startsWith('$prefix/') || topic == prefix;
    }

    if (pattern == '#') {
      return true;
    }

    // Exact match
    return pattern == topic;
  }

  /// Get statistics about retained messages
  Map<String, dynamic> getStats() {
    final stats = <String, dynamic>{
      'totalMessages': _retainedMessages.length,
      'totalPayloadSize': 0,
      'topics': <String>[],
    };

    int totalSize = 0;
    for (final message in _retainedMessages.values) {
      totalSize += message.payload.length;
    }

    stats['totalPayloadSize'] = totalSize;
    stats['topics'] = _retainedMessages.keys.toList();

    return stats;
  }

  @override
  String toString() {
    return 'RetainedMessageStore(${_retainedMessages.length} messages)';
  }
}
