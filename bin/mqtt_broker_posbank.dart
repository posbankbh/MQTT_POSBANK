import 'dart:io';
import 'dart:async';
import 'package:mqtt_broker_posbank/mqtt_broker.dart';

void main(List<String> arguments) async {
  // Parse command line arguments
  int port = 1887;
  String host = '0.0.0.0';
  bool showHelp = false;

  for (int i = 0; i < arguments.length; i++) {
    switch (arguments[i]) {
      case '-p':
      case '--port':
        if (i + 1 < arguments.length) {
          port = int.tryParse(arguments[++i]) ?? 1883;
        }
        break;
      case '-h':
      case '--host':
        if (i + 1 < arguments.length) {
          host = arguments[++i];
        }
        break;
      case '--help':
        showHelp = true;
        break;
    }
  }

  if (showHelp) {
    _printHelp();
    return;
  }

  print('MQTT Broker for PosBank');
  print('======================');
  print('Starting MQTT broker on $host:$port');
  print('Features:');
  print('- TCP connections (insecure)');
  print('- QoS 0, 1, 2 support');
  print('- Retained messages');
  print('- Client disconnect handling');
  print('- Keep-alive monitoring');
  print('');

  // Create and start the broker
  final broker = MqttBroker(host: host, port: port);

  try {
    await broker.start();

    // Setup signal handlers for graceful shutdown
    _setupSignalHandlers(broker);

    // Print status every 30 seconds
    Timer.periodic(Duration(seconds: 30), (timer) {
      _printStatus(broker);
    });

    print('MQTT Broker is running. Press Ctrl+C to stop.');
    print('Use MQTT client to connect on $host:$port');
    print('');

    // Keep the application running
    await _waitForShutdown();
  } catch (e) {
    print('Error starting broker: $e');
    exit(1);
  } finally {
    await broker.stop();
    print('Broker stopped.');
  }
}

void _printHelp() {
  print('MQTT Broker for PosBank');
  print('');
  print('Usage: dart run bin/mqtt_broker_posbank.dart [options]');
  print('');
  print('Options:');
  print('  -p, --port PORT     Set the port to listen on (default: 1886)');
  print('  -h, --host HOST     Set the host to bind to (default: 0.0.0.0)');
  print('  --help              Show this help message');
  print('');
  print('Examples:');
  print('  dart run bin/mqtt_broker_posbank.dart');
  print('  dart run bin/mqtt_broker_posbank.dart -p 8883');
  print('  dart run bin/mqtt_broker_posbank.dart -h localhost -p 1884');
}

void _setupSignalHandlers(MqttBroker broker) {
  // Handle SIGINT (Ctrl+C) - supported on all platforms
  ProcessSignal.sigint.watch().listen((signal) async {
    print('\nReceived SIGINT, shutting down gracefully...');
    await broker.stop();
    exit(0);
  });

  // Handle SIGTERM only on platforms that support it (Unix-like systems)
  if (!Platform.isWindows) {
    try {
      ProcessSignal.sigterm.watch().listen((signal) async {
        print('\nReceived SIGTERM, shutting down gracefully...');
        await broker.stop();
        exit(0);
      });
    } catch (e) {
      print('Warning: SIGTERM handling not available: $e');
    }
  }
}

void _printStatus(MqttBroker broker) {
  final stats = broker.getStats();
  final clients = broker.getConnectedClients();

  print('--- MQTT Broker Status ---');
  print('Connected clients: ${stats['currentConnections']}');
  print('Total connections: ${stats['totalConnections']}');
  print('Messages published: ${stats['messagesPublished']}');
  print('Messages delivered: ${stats['messagesDelivered']}');
  print('Retained messages: ${stats['retainedMessages']}');

  if (clients.isNotEmpty) {
    print('Active clients: ${clients.join(', ')}');
  }
  print('-------------------------');
}

Future<void> _waitForShutdown() async {
  // Create a completer that will complete when we want to shutdown
  final completer = Completer<void>();

  // This will keep the application running until explicitly completed
  // The signal handlers will complete this when shutdown is requested
  return completer.future;
}
