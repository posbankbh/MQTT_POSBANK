import 'dart:io';
import 'dart:typed_data';
import 'dart:async';
import 'mqtt_message.dart';
import 'mqtt_message_buffer.dart';

/// Client session state for persistence
class ClientSession {
  final Map<String, MqttQoS> subscriptions = {};
  final Map<int, MqttPublishMessage> pendingPublishes = {};
  final Map<int, MqttPublishMessage> inFlightMessages = {};
  final Set<int> receivedQoS2PacketIds = {};
  int lastPacketId = 0;

  void clear() {
    subscriptions.clear();
    pendingPublishes.clear();
    inFlightMessages.clear();
    receivedQoS2PacketIds.clear();
    lastPacketId = 0;
  }
}

/// Represents a connected MQTT client
class MqttClient {
  final Socket socket;
  final String clientId;
  final bool cleanSession;
  final int keepAlive;
  final DateTime connectedAt;

  /// Will message information
  final String? willTopic;
  final String? willMessage;
  final MqttQoS willQoS;
  final bool willRetain;

  /// Session data (persisted if cleanSession = false)
  final ClientSession session = ClientSession();

  /// Message buffer for handling TCP fragmentation
  final MqttMessageBuffer _messageBuffer = MqttMessageBuffer();

  /// Last activity timestamp for keep-alive tracking
  DateTime lastActivity;

  /// Last time we sent data (for keep-alive)
  DateTime lastSentActivity;

  /// Client state
  bool isConnected = true;
  bool isCleanDisconnect = false;

  /// Stream controller for incoming messages
  final StreamController<MqttMessage> _messageController = StreamController<MqttMessage>.broadcast();

  /// Stream of incoming messages
  Stream<MqttMessage> get messageStream => _messageController.stream;

  /// Stream controller for disconnection events
  final StreamController<MqttClient> _disconnectController = StreamController<MqttClient>.broadcast();

  /// Stream of disconnection events
  Stream<MqttClient> get disconnectStream => _disconnectController.stream;

  MqttClient({
    required this.socket,
    required this.clientId,
    required this.cleanSession,
    required this.keepAlive,
    this.willTopic,
    this.willMessage,
    this.willQoS = MqttQoS.atMostOnce,
    this.willRetain = false,
    bool startListening = true,
  })  : connectedAt = DateTime.now(),
        lastActivity = DateTime.now(),
        lastSentActivity = DateTime.now() {
    // Clear session if clean session requested
    if (cleanSession) {
      session.clear();
    }

    if (startListening) {
      _startListening();
    }
  }

  /// Get next packet ID, ensuring no collision with pending messages
  int getNextPacketId() {
    int attempts = 0;
    do {
      session.lastPacketId = (session.lastPacketId + 1) % 65536;
      if (session.lastPacketId == 0) session.lastPacketId = 1; // Packet ID 0 is reserved
      attempts++;

      // Avoid infinite loop if all packet IDs are somehow in use
      if (attempts > 65535) {
        throw Exception('No available packet IDs');
      }
    } while (session.pendingPublishes.containsKey(session.lastPacketId) || session.inFlightMessages.containsKey(session.lastPacketId));

    return session.lastPacketId;
  }

  /// Send a message to the client
  Future<void> sendMessage(MqttMessage message) async {
    if (!isConnected) return;

    try {
      final bytes = message.toBytes();
      socket.add(bytes);
      await socket.flush();
      lastSentActivity = DateTime.now();
    } catch (e) {
      print('Error sending message to client $clientId: $e');
      await disconnect();
    }
  }

  /// Subscribe to a topic
  void subscribe(String topicFilter, MqttQoS qos) {
    if (!MqttMessage.isValidTopicFilter(topicFilter)) {
      throw Exception('Invalid topic filter: $topicFilter');
    }
    session.subscriptions[topicFilter] = qos;
  }

  /// Unsubscribe from a topic
  void unsubscribe(String topicFilter) {
    session.subscriptions.remove(topicFilter);
  }

  /// Check if client is subscribed to a topic
  bool isSubscribedTo(String topic) {
    for (final pattern in session.subscriptions.keys) {
      if (_matchesTopic(pattern, topic)) {
        return true;
      }
    }
    return false;
  }

  /// Get QoS level for a topic
  MqttQoS getTopicQoS(String topic) {
    for (final entry in session.subscriptions.entries) {
      if (_matchesTopic(entry.key, topic)) {
        return entry.value;
      }
    }
    return MqttQoS.atMostOnce;
  }

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

  /// Add pending publish for QoS 1/2
  void addPendingPublish(int packetId, MqttPublishMessage message) {
    session.pendingPublishes[packetId] = message;
  }

  /// Remove pending publish
  void removePendingPublish(int packetId) {
    session.pendingPublishes.remove(packetId);
  }

  /// Get pending publish
  MqttPublishMessage? getPendingPublish(int packetId) {
    return session.pendingPublishes[packetId];
  }

  /// Add in-flight message for QoS 2
  void addInFlightMessage(int packetId, MqttPublishMessage message) {
    session.inFlightMessages[packetId] = message;
  }

  /// Remove in-flight message
  void removeInFlightMessage(int packetId) {
    session.inFlightMessages.remove(packetId);
  }

  /// Get in-flight message
  MqttPublishMessage? getInFlightMessage(int packetId) {
    return session.inFlightMessages[packetId];
  }

  /// Check if client has timed out (keep-alive)
  bool hasTimedOut() {
    if (keepAlive == 0) return false; // No keep-alive

    final timeout = Duration(seconds: (keepAlive * 1.5).round());

    // For keep-alive timeout, we track incoming activity as the client
    // is responsible for sending PINGREQ to keep connection alive
    return DateTime.now().difference(lastActivity) > timeout;
  }

  /// Start listening for incoming data
  void _startListening() {
    socket.listen(
      _handleData,
      onError: _handleError,
      onDone: _handleDone,
    );
  }

  /// Manually start listening (for cases where socket is already being listened to)
  void startListening() {
    _startListening();
  }

  /// Set up client with existing message buffer and stream subscription
  void setupWithSubscription(StreamSubscription<Uint8List> subscription, MqttMessageBuffer existingBuffer) {
    // Transfer any remaining data from handshake buffer to our buffer
    if (existingBuffer.bufferSize > 0) {
      // We need to transfer the existing buffer data
      // For now, we'll process any remaining messages
      final remainingMessages = existingBuffer.extractMessages();
      for (final message in remainingMessages) {
        if (!_messageController.isClosed) {
          _messageController.add(message);
        }
      }
    }

    // Take over the existing subscription
    subscription.onData(_handleData);
    subscription.onError(_handleError);
    subscription.onDone(_handleDone);
  }

  /// Handle incoming data with proper buffering
  void _handleData(Uint8List data) {
    lastActivity = DateTime.now();

    try {
      // Check for buffer overflow protection
      if (_messageBuffer.isBufferOverflow) {
        print('Buffer overflow detected from client $clientId, disconnecting');
        disconnect();
        return;
      }

      // Add data to buffer
      _messageBuffer.addData(data);

      // Extract complete messages
      final messages = _messageBuffer.extractMessages();
      for (final message in messages) {
        if (!_messageController.isClosed) {
          _messageController.add(message);
        }
      }
    } catch (e) {
      print('Error processing data from client $clientId: $e');
      disconnect();
    }
  }

  /// Handle socket errors
  void _handleError(error) {
    print('Socket error for client $clientId: $error');
    disconnect();
  }

  /// Handle socket disconnection
  void _handleDone() {
    print('Client $clientId disconnected');
    disconnect();
  }

  /// Disconnect the client
  Future<void> disconnect() async {
    if (!isConnected) return;

    isConnected = false;

    try {
      await socket.close();
    } catch (e) {
      print('Error closing socket for client $clientId: $e');
    }

    // Clean up resources
    _messageBuffer.clear();

    // Clear session if clean session
    if (cleanSession) {
      session.clear();
    }

    // Notify disconnection
    if (!_disconnectController.isClosed) {
      _disconnectController.add(this);
    }

    // Close stream controllers safely
    if (!_messageController.isClosed) {
      await _messageController.close();
    }
    if (!_disconnectController.isClosed) {
      await _disconnectController.close();
    }
  }

  /// Get all subscriptions
  Map<String, MqttQoS> get subscriptions => Map.unmodifiable(session.subscriptions);

  /// Get client info for debugging
  Map<String, dynamic> getInfo() {
    return {
      'clientId': clientId,
      'cleanSession': cleanSession,
      'keepAlive': keepAlive,
      'connectedAt': connectedAt.toIso8601String(),
      'lastActivity': lastActivity.toIso8601String(),
      'lastSentActivity': lastSentActivity.toIso8601String(),
      'isConnected': isConnected,
      'subscriptions': session.subscriptions.map((k, v) => MapEntry(k, v.value)),
      'pendingPublishes': session.pendingPublishes.length,
      'inFlightMessages': session.inFlightMessages.length,
      'bufferSize': _messageBuffer.bufferSize,
      'remoteAddress': socket.remoteAddress.address,
      'remotePort': socket.remotePort,
    };
  }

  @override
  String toString() {
    return 'MqttClient($clientId, ${socket.remoteAddress.address}:${socket.remotePort})';
  }
}
