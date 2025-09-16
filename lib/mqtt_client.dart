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

  /// Cached subscription patterns for performance optimization
  List<String>? _cachedSubscriptionPatterns;

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
  late final StreamController<MqttMessage> _messageController;

  /// Stream of incoming messages
  Stream<MqttMessage> get messageStream => _messageController.stream;

  /// Stream controller for disconnection events
  late final StreamController<MqttClient> _disconnectController;

  /// Stream of disconnection events
  Stream<MqttClient> get disconnectStream => _disconnectController.stream;

  /// Track if controllers are initialized
  bool _controllersInitialized = false;

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
    // Initialize stream controllers
    _initializeControllers();

    // Clear session if clean session requested
    if (cleanSession) {
      session.clear();
    }

    if (startListening) {
      _startListening();
    }
  }

  /// Initialize stream controllers safely
  void _initializeControllers() {
    if (!_controllersInitialized) {
      _messageController = StreamController<MqttMessage>.broadcast();
      _disconnectController = StreamController<MqttClient>.broadcast();
      _controllersInitialized = true;
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
    _invalidateSubscriptionCache();
  }

  /// Unsubscribe from a topic
  void unsubscribe(String topicFilter) {
    session.subscriptions.remove(topicFilter);
    _invalidateSubscriptionCache();
  }

  /// Invalidate the subscription pattern cache
  void _invalidateSubscriptionCache() {
    _cachedSubscriptionPatterns = null;
  }

  /// Get cached subscription patterns (sorted by complexity for performance)
  List<String> _getSubscriptionPatterns() {
    if (_cachedSubscriptionPatterns == null) {
      _cachedSubscriptionPatterns = session.subscriptions.keys.toList();

      // Sort patterns for better matching performance:
      // 1. Exact matches first (no wildcards)
      // 2. Single-level wildcards (+) next
      // 3. Multi-level wildcards (#) last
      _cachedSubscriptionPatterns!.sort((a, b) {
        final aScore = _getPatternComplexity(a);
        final bScore = _getPatternComplexity(b);
        return aScore.compareTo(bScore);
      });
    }
    return _cachedSubscriptionPatterns!;
  }

  /// Get pattern complexity score for sorting (lower = simpler/faster to match)
  int _getPatternComplexity(String pattern) {
    if (!pattern.contains('+') && !pattern.contains('#')) {
      return 0; // Exact match - fastest
    } else if (pattern.contains('+') && !pattern.contains('#')) {
      return 1; // Single-level wildcard
    } else {
      return 2; // Multi-level wildcard - slowest
    }
  }

  /// Check if client is subscribed to a topic (optimized)
  bool isSubscribedTo(String topic) {
    // Early return for empty subscriptions
    if (session.subscriptions.isEmpty) return false;

    // Use sorted patterns for faster matching
    for (final pattern in _getSubscriptionPatterns()) {
      if (MqttMessage.matchesTopic(pattern, topic)) {
        return true;
      }
    }
    return false;
  }

  /// Get QoS level for a topic (optimized)
  MqttQoS getTopicQoS(String topic) {
    // Early return for empty subscriptions
    if (session.subscriptions.isEmpty) return MqttQoS.atMostOnce;

    // Use sorted patterns for faster matching - return first match
    for (final pattern in _getSubscriptionPatterns()) {
      if (MqttMessage.matchesTopic(pattern, topic)) {
        return session.subscriptions[pattern]!;
      }
    }
    return MqttQoS.atMostOnce;
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
      // Don't disconnect for buffer-related exceptions that are handled internally
      if (e.toString().contains('Message too large') || e.toString().contains('Total message size exceeds limit')) {
        print('Message size limit exceeded, continuing with next message');
        // Clear buffer to recover from oversized message
        _messageBuffer.clear();
      } else {
        disconnect();
      }
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

    // Notify disconnection before closing controllers
    if (_controllersInitialized && !_disconnectController.isClosed) {
      _disconnectController.add(this);

      // Give listeners a chance to process the disconnection
      await Future.delayed(Duration(milliseconds: 10));
    }

    // Close stream controllers safely with proper cleanup
    await _closeControllers();
  }

  /// Safely close stream controllers
  Future<void> _closeControllers() async {
    if (!_controllersInitialized) return;

    try {
      // Close message controller first
      if (!_messageController.isClosed) {
        await _messageController.close();
      }
    } catch (e) {
      print('Error closing message controller for client $clientId: $e');
    }

    try {
      // Close disconnect controller
      if (!_disconnectController.isClosed) {
        await _disconnectController.close();
      }
    } catch (e) {
      print('Error closing disconnect controller for client $clientId: $e');
    }

    _controllersInitialized = false;
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
