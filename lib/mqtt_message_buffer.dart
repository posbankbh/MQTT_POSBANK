import 'dart:typed_data';
import 'mqtt_message.dart';

/// Handles buffering and parsing of MQTT messages from TCP stream
class MqttMessageBuffer {
  final List<int> _buffer = [];

  /// Add incoming TCP data to the buffer
  void addData(Uint8List data) {
    _buffer.addAll(data);
  }

  /// Try to extract complete MQTT messages from the buffer
  List<MqttMessage> extractMessages() {
    final messages = <MqttMessage>[];

    while (_buffer.isNotEmpty) {
      final message = _tryParseMessage();
      if (message == null) break; // Need more data
      messages.add(message);
    }

    return messages;
  }

  /// Try to parse a single message from the buffer
  MqttMessage? _tryParseMessage() {
    if (_buffer.isEmpty) return null;

    // Need at least 2 bytes for fixed header
    if (_buffer.length < 2) return null;

    // Parse remaining length
    int remainingLengthBytes = 0;
    int remainingLength = 0;
    int multiplier = 1;
    int index = 1;

    do {
      if (index >= _buffer.length) return null; // Need more data

      final digit = _buffer[index++];
      remainingLength += (digit & 127) * multiplier;
      remainingLengthBytes++;

      if ((digit & 128) == 0) break;
      multiplier *= 128;

      if (multiplier > 128 * 128 * 128) {
        // Malformed remaining length - try to resynchronize
        _resynchronizeBuffer();
        return _tryParseMessage();
      }
    } while (true);

    // Check individual message size limit before processing
    if (remainingLength > maxMqttMessageSize) {
      throw Exception('Message too large: $remainingLength bytes (max: $maxMqttMessageSize)');
    }

    // Calculate total message length
    final totalLength = 1 + remainingLengthBytes + remainingLength;

    // Check if we have the complete message
    if (_buffer.length < totalLength) return null;

    // Additional safety check for total message length
    if (totalLength > maxMqttMessageSize + 5) {
      // +5 for max header overhead
      throw Exception('Total message size exceeds limit: $totalLength bytes');
    }

    // Extract message data
    final messageData = Uint8List.fromList(_buffer.take(totalLength).toList());

    // Remove processed data from buffer
    _buffer.removeRange(0, totalLength);

    // Parse the message
    try {
      return MqttMessage.fromBytes(messageData);
    } catch (e) {
      // If parsing fails, the message was malformed
      // Try to resynchronize and continue
      _resynchronizeBuffer();
      return _tryParseMessage();
    }
  }

  /// Get current buffer size
  int get bufferSize => _buffer.length;

  /// Clear the buffer
  void clear() {
    _buffer.clear();
  }

  /// Maximum MQTT message size (256MB - per MQTT spec)
  static const int maxMqttMessageSize = 268435455;

  /// Maximum buffer size before considering it an overflow (512KB)
  static const int maxBufferSize = 524288;

  /// Check if buffer is getting too large (potential DoS protection)
  bool get isBufferOverflow => _buffer.length > maxBufferSize;

  /// Resynchronize buffer after malformed message detection
  void _resynchronizeBuffer() {
    // Look for next potential MQTT message start
    // MQTT messages start with specific bit patterns in first byte
    for (int i = 1; i < _buffer.length; i++) {
      final firstByte = _buffer[i];
      final messageType = (firstByte >> 4) & 0x0F;

      // Check if this could be a valid MQTT message type (1-14)
      if (messageType >= 1 && messageType <= 14) {
        // Remove bytes before this potential message start
        _buffer.removeRange(0, i);
        return;
      }
    }

    // No valid message start found, clear buffer except last few bytes
    // Keep last 3 bytes in case they're part of a message arriving
    if (_buffer.length > 3) {
      final keepBytes = _buffer.sublist(_buffer.length - 3);
      _buffer.clear();
      _buffer.addAll(keepBytes);
    }
  }
}
