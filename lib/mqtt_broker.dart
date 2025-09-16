import 'dart:io';
import 'dart:async';
import 'dart:typed_data';
import 'dart:math' as math;
import 'mqtt_client.dart';
import 'mqtt_message.dart';
import 'mqtt_message_buffer.dart';
import 'retained_message_store.dart';

/// MQTT Broker implementation
class MqttBroker {
  final int port;
  final String host;

  /// Connected clients
  final Map<String, MqttClient> clients = {};

  /// Persistent sessions for non-clean session clients
  final Map<String, ClientSession> persistentSessions = {};

  /// Retained message store
  final RetainedMessageStore retainedMessages = RetainedMessageStore();

  /// Server socket
  ServerSocket? _serverSocket;

  /// Keep-alive timer
  Timer? _keepAliveTimer;

  /// Broker statistics
  final Map<String, int> stats = {
    'totalConnections': 0,
    'currentConnections': 0,
    'messagesPublished': 0,
    'messagesDelivered': 0,
    'bytesReceived': 0,
    'bytesSent': 0,
  };

  MqttBroker({
    this.port = 1883,
    this.host = '0.0.0.0',
  });

  /// Start the MQTT broker
  Future<void> start() async {
    try {
      _serverSocket = await ServerSocket.bind(host, port);
      print('MQTT Broker started on $host:$port');

      // Start keep-alive monitoring
      _startKeepAliveMonitoring();

      // Listen for incoming connections
      _serverSocket!.listen(
        _handleNewConnection,
        onError: (error) {
          print('Server error: $error');
        },
        onDone: () {
          print('Server closed');
        },
      );
    } catch (e) {
      print('Failed to start MQTT broker: $e');
      rethrow;
    }
  }

  /// Stop the MQTT broker
  Future<void> stop() async {
    print('Stopping MQTT broker...');

    // Stop keep-alive monitoring
    _keepAliveTimer?.cancel();

    // Disconnect all clients
    final clientList = clients.values.toList();
    for (final client in clientList) {
      await client.disconnect();
    }
    clients.clear();

    // Close server socket
    await _serverSocket?.close();
    _serverSocket = null;

    print('MQTT broker stopped');
  }

  /// Handle new client connection
  void _handleNewConnection(Socket socket) {
    print('New connection from ${socket.remoteAddress.address}:${socket.remotePort}');
    stats['totalConnections'] = (stats['totalConnections'] ?? 0) + 1;

    // Create a temporary client handler until we receive CONNECT
    _handleClientHandshake(socket);
  }

  /// Handle client handshake (CONNECT message)
  void _handleClientHandshake(Socket socket) {
    late StreamSubscription<Uint8List> subscription;
    final timeout = Timer(Duration(seconds: 30), () {
      print('Connection timeout from ${socket.remoteAddress.address}:${socket.remotePort}');
      socket.close();
    });

    final messageBuffer = MqttMessageBuffer();

    subscription = socket.listen(
      (Uint8List data) {
        try {
          messageBuffer.addData(data);
          final messages = messageBuffer.extractMessages();

          if (messages.isNotEmpty) {
            timeout.cancel();
            // Don't cancel subscription - pass it to the client

            final message = messages.first;
            if (message is MqttConnectMessage) {
              _handleConnectMessage(socket, message, messageBuffer, subscription);
            } else {
              print('Expected CONNECT message, got ${message.messageType}');
              subscription.cancel();
              socket.close();
            }
          }
        } catch (e) {
          timeout.cancel();
          subscription.cancel();
          print('Error parsing CONNECT message: $e');
          socket.close();
        }
      },
      onError: (error) {
        timeout.cancel();
        subscription.cancel();
        print('Handshake error: $error');
        socket.close();
      },
      onDone: () {
        timeout.cancel();
        subscription.cancel();
        print('Connection closed during handshake');
      },
    );
  }

  /// Handle CONNECT message
  Future<void> _handleConnectMessage(
      Socket socket, MqttConnectMessage connectMessage, MqttMessageBuffer messageBuffer, StreamSubscription<Uint8List> subscription) async {
    final clientId = connectMessage.clientId;

    // Validate client ID
    if (clientId.isEmpty) {
      // Generate client ID or reject
      final connack = MqttConnackMessage(returnCode: 2); // Identifier rejected
      socket.add(connack.toBytes());
      await socket.flush();
      socket.close();
      return;
    }

    // Check if client is already connected
    final existingClient = clients[clientId];
    bool sessionPresent = false;

    if (existingClient != null) {
      // Client takeover - disconnect existing connection
      print('Client takeover for $clientId');
      await existingClient.disconnect();
      clients.remove(clientId);
    }

    // Check for persistent session
    if (!connectMessage.cleanSession) {
      sessionPresent = persistentSessions.containsKey(clientId);
    } else {
      // Clean session - remove any persistent session
      persistentSessions.remove(clientId);
    }

    // Create new client without starting to listen (we'll handle that manually)
    final client = MqttClient(
      socket: socket,
      clientId: clientId,
      cleanSession: connectMessage.cleanSession,
      keepAlive: connectMessage.keepAlive,
      startListening: false,
    );

    // Restore persistent session if available
    if (!connectMessage.cleanSession && persistentSessions.containsKey(clientId)) {
      final persistentSession = persistentSessions[clientId]!;
      // Copy persistent session data to client
      client.session.subscriptions.addAll(persistentSession.subscriptions);
      client.session.pendingPublishes.addAll(persistentSession.pendingPublishes);
      client.session.inFlightMessages.addAll(persistentSession.inFlightMessages);
      client.session.lastPacketId = persistentSession.lastPacketId;
    }

    // Add client to active clients
    clients[clientId] = client;
    stats['currentConnections'] = clients.length;

    // Send CONNACK first (required by MQTT spec)
    final connack = MqttConnackMessage(
      sessionPresent: sessionPresent,
      returnCode: 0, // Connection accepted
    );
    await client.sendMessage(connack);

    print('Client $clientId connected (clean session: ${connectMessage.cleanSession}, session present: $sessionPresent)');

    // Set up client with existing message buffer and subscription
    client.setupWithSubscription(subscription, messageBuffer);

    // Setup client message handling
    _setupClientHandling(client);

    // Resend QoS 1 and 2 messages if session present (after CONNACK)
    if (sessionPresent) {
      // Small delay to ensure CONNACK is processed before resending messages
      await Future.delayed(Duration(milliseconds: 10));
      await _resendPendingMessages(client);
    }
  }

  /// Resend pending QoS 1 and 2 messages for restored session
  Future<void> _resendPendingMessages(MqttClient client) async {
    // Resend pending publishes (QoS 1 and 2)
    for (final message in client.session.pendingPublishes.values) {
      final resendMessage = MqttPublishMessage(
        topic: message.topic,
        packetId: message.packetId,
        payload: message.payload,
        dup: true, // Set DUP flag for resent messages
        qos: message.qos,
        retain: false,
      );
      await client.sendMessage(resendMessage);
    }
  }

  /// Setup message handling for a client
  void _setupClientHandling(MqttClient client) {
    // Listen for incoming messages
    client.messageStream.listen(
      (message) => _handleClientMessage(client, message),
      onError: (error) {
        print('Error handling message from ${client.clientId}: $error');
      },
    );

    // Listen for disconnections
    client.disconnectStream.listen(
      (disconnectedClient) {
        _handleClientDisconnect(disconnectedClient);
      },
    );
  }

  /// Handle message from client
  Future<void> _handleClientMessage(MqttClient client, MqttMessage message) async {
    stats['messagesPublished'] = (stats['messagesPublished'] ?? 0) + 1;

    switch (message.messageType) {
      case MqttMessageType.publish:
        await _handlePublishMessage(client, message as MqttPublishMessage);
        break;
      case MqttMessageType.subscribe:
        await _handleSubscribeMessage(client, message as MqttSubscribeMessage);
        break;
      case MqttMessageType.unsubscribe:
        await _handleUnsubscribeMessage(client, message as MqttUnsubscribeMessage);
        break;
      case MqttMessageType.puback:
        await _handlePubackMessage(client, message as MqttPubackMessage);
        break;
      case MqttMessageType.pubrec:
        await _handlePubrecMessage(client, message as MqttPubrecMessage);
        break;
      case MqttMessageType.pubrel:
        await _handlePubrelMessage(client, message as MqttPubrelMessage);
        break;
      case MqttMessageType.pubcomp:
        await _handlePubcompMessage(client, message as MqttPubcompMessage);
        break;
      case MqttMessageType.pingreq:
        await _handlePingreqMessage(client, message as MqttPingreqMessage);
        break;
      case MqttMessageType.disconnect:
        await _handleDisconnectMessage(client, message as MqttDisconnectMessage);
        break;
      default:
        print('Unhandled message type: ${message.messageType}');
    }
  }

  /// Handle PUBLISH message
  Future<void> _handlePublishMessage(MqttClient client, MqttPublishMessage message) async {
    // Validate topic
    if (!MqttMessage.isValidTopicName(message.topic)) {
      print('Invalid topic name from client ${client.clientId}: ${message.topic}');
      return;
    }

    print('Publishing message on topic "${message.topic}" from client ${client.clientId}');

    // Store retained message if needed
    if (message.retain) {
      retainedMessages.storeMessage(message.topic, message);
    }

    // Handle QoS flow
    switch (message.qos) {
      case MqttQoS.atMostOnce:
        // QoS 0 - no acknowledgment needed
        await _distributeMessage(message, client);
        break;
      case MqttQoS.atLeastOnce:
        // QoS 1 - send PUBACK
        if (message.packetId != null) {
          final puback = MqttPubackMessage(packetId: message.packetId!);
          await client.sendMessage(puback);
          await _distributeMessage(message, client);
        }
        break;
      case MqttQoS.exactlyOnce:
        // QoS 2 - send PUBREC, start handshake
        if (message.packetId != null) {
          client.addInFlightMessage(message.packetId!, message);
          final pubrec = MqttPubrecMessage(packetId: message.packetId!);
          await client.sendMessage(pubrec);
          // Message will be distributed when PUBREL is received
        }
        break;
      case MqttQoS.reserved:
        print('Reserved QoS level used');
        return;
    }
  }

  /// Handle SUBSCRIBE message
  Future<void> _handleSubscribeMessage(MqttClient client, MqttSubscribeMessage message) async {
    print('Client ${client.clientId} subscribing to ${message.topics}');

    final returnCodes = <int>[];

    for (int i = 0; i < message.topics.length; i++) {
      final topic = message.topics[i];
      final qos = message.qosLevels[i];

      // Validate topic filter
      if (!MqttMessage.isValidTopicFilter(topic)) {
        returnCodes.add(0x80); // Failure
        continue;
      }

      // Add subscription
      client.subscribe(topic, qos);

      // Return the granted QoS level
      returnCodes.add(qos.value);

      // Send retained messages that match the subscription
      final retainedMatches = retainedMessages.getMatchingMessages(topic);
      for (final retainedMessage in retainedMatches) {
        // Determine the QoS to use (minimum of subscription QoS and message QoS)
        final deliveryQoS = MqttQoS.fromValue(math.min(qos.value, retainedMessage.qos.value));

        final deliveryMessage = MqttPublishMessage(
          topic: retainedMessage.topic,
          packetId: deliveryQoS != MqttQoS.atMostOnce ? client.getNextPacketId() : null,
          payload: retainedMessage.payload,
          qos: deliveryQoS,
          retain: true,
        );

        await _deliverMessageToClient(client, deliveryMessage);
      }
    }

    // Send SUBACK
    final suback = MqttSubackMessage(
      packetId: message.packetId,
      returnCodes: returnCodes,
    );
    await client.sendMessage(suback);
  }

  /// Handle UNSUBSCRIBE message
  Future<void> _handleUnsubscribeMessage(MqttClient client, MqttUnsubscribeMessage message) async {
    print('Client ${client.clientId} unsubscribing from ${message.topics}');

    for (final topic in message.topics) {
      client.unsubscribe(topic);
    }

    // Send UNSUBACK
    final unsuback = MqttUnsubackMessage(packetId: message.packetId);
    await client.sendMessage(unsuback);
  }

  /// Handle PUBACK message (QoS 1 acknowledgment from client)
  Future<void> _handlePubackMessage(MqttClient client, MqttPubackMessage message) async {
    print('Received PUBACK from ${client.clientId} for packet ${message.packetId}');
    client.removePendingPublish(message.packetId);
  }

  /// Handle PUBREC message (QoS 2 received from client)
  Future<void> _handlePubrecMessage(MqttClient client, MqttPubrecMessage message) async {
    print('Received PUBREC from ${client.clientId} for packet ${message.packetId}');

    // This is the broker receiving PUBREC for a message we sent to the client
    // We should send PUBREL in response
    final pendingMessage = client.getPendingPublish(message.packetId);
    if (pendingMessage != null) {
      // Move from pending to in-flight and send PUBREL
      client.removePendingPublish(message.packetId);
      client.addInFlightMessage(message.packetId, pendingMessage);

      final pubrel = MqttPubrelMessage(packetId: message.packetId);
      await client.sendMessage(pubrel);
    }
  }

  /// Handle PUBREL message (QoS 2 release from client)
  Future<void> _handlePubrelMessage(MqttClient client, MqttPubrelMessage message) async {
    print('Received PUBREL from ${client.clientId} for packet ${message.packetId}');

    // Get the in-flight message and complete delivery
    final publishMessage = client.getInFlightMessage(message.packetId);
    if (publishMessage != null) {
      await _distributeMessage(publishMessage, client);
      client.removeInFlightMessage(message.packetId);
    }

    // Send PUBCOMP
    final pubcomp = MqttPubcompMessage(packetId: message.packetId);
    await client.sendMessage(pubcomp);
  }

  /// Handle PUBCOMP message (QoS 2 complete from client)
  Future<void> _handlePubcompMessage(MqttClient client, MqttPubcompMessage message) async {
    print('Received PUBCOMP from ${client.clientId} for packet ${message.packetId}');
    // Complete the QoS 2 flow - remove from in-flight
    client.removeInFlightMessage(message.packetId);
  }

  /// Handle PINGREQ message
  Future<void> _handlePingreqMessage(MqttClient client, MqttPingreqMessage message) async {
    // Send PINGRESP
    final pingresp = MqttPingrespMessage();
    await client.sendMessage(pingresp);
  }

  /// Handle DISCONNECT message
  Future<void> _handleDisconnectMessage(MqttClient client, MqttDisconnectMessage message) async {
    print('Client ${client.clientId} sent DISCONNECT');
    await client.disconnect();
  }

  /// Distribute message to all subscribed clients
  Future<void> _distributeMessage(MqttPublishMessage message, MqttClient publisher) async {
    final topic = message.topic;

    for (final client in clients.values) {
      if (client == publisher) continue; // Don't send back to publisher
      if (!client.isSubscribedTo(topic)) continue;

      await _deliverMessageToClient(client, message);
    }
  }

  /// Deliver message to a specific client
  Future<void> _deliverMessageToClient(MqttClient client, MqttPublishMessage message) async {
    // Determine the QoS to use (minimum of subscription QoS and message QoS)
    final subscriptionQoS = client.getTopicQoS(message.topic);
    final deliveryQoS = MqttQoS.fromValue(math.min(subscriptionQoS.value, message.qos.value));

    // Create delivery message
    final deliveryMessage = MqttPublishMessage(
      topic: message.topic,
      packetId: deliveryQoS != MqttQoS.atMostOnce ? client.getNextPacketId() : null,
      payload: message.payload,
      qos: deliveryQoS,
      retain: false, // Retain flag is not set for normal delivery
    );

    // Track pending messages for QoS 1 and 2
    if (deliveryMessage.packetId != null) {
      client.addPendingPublish(deliveryMessage.packetId!, deliveryMessage);
    }

    await client.sendMessage(deliveryMessage);
    stats['messagesDelivered'] = (stats['messagesDelivered'] ?? 0) + 1;
  }

  /// Handle client disconnection
  void _handleClientDisconnect(MqttClient client) {
    print('Client ${client.clientId} disconnected');

    // Save persistent session if needed
    if (!client.cleanSession) {
      persistentSessions[client.clientId] = ClientSession()
        ..subscriptions.addAll(client.session.subscriptions)
        ..pendingPublishes.addAll(client.session.pendingPublishes)
        ..inFlightMessages.addAll(client.session.inFlightMessages)
        ..lastPacketId = client.session.lastPacketId;
    }

    clients.remove(client.clientId);
    stats['currentConnections'] = clients.length;
  }

  /// Start keep-alive monitoring
  void _startKeepAliveMonitoring() {
    _keepAliveTimer = Timer.periodic(Duration(seconds: 30), (timer) {
      final clientsToRemove = <String>[];

      for (final client in clients.values) {
        if (client.hasTimedOut()) {
          print('Client ${client.clientId} timed out (keep-alive)');
          clientsToRemove.add(client.clientId);
          client.disconnect();
        }
      }

      for (final clientId in clientsToRemove) {
        clients.remove(clientId);
      }

      if (clientsToRemove.isNotEmpty) {
        stats['currentConnections'] = clients.length;
      }
    });
  }

  /// Get broker statistics
  Map<String, dynamic> getStats() {
    return {
      ...stats,
      'retainedMessages': retainedMessages.count,
      'persistentSessions': persistentSessions.length,
      'clients': clients.values.map((c) => c.getInfo()).toList(),
    };
  }

  /// Get information about a specific client
  Map<String, dynamic>? getClientInfo(String clientId) {
    final client = clients[clientId];
    return client?.getInfo();
  }

  /// Get list of all connected clients
  List<String> getConnectedClients() {
    return clients.keys.toList();
  }

  /// Forcibly disconnect a client
  Future<void> disconnectClient(String clientId) async {
    final client = clients[clientId];
    if (client != null) {
      await client.disconnect();
    }
  }

  @override
  String toString() {
    return 'MqttBroker($host:$port, ${clients.length} clients)';
  }
}
