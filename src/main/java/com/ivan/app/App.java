package com.ivan.app;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ServiceBusException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Properties;

/**
 * Created by Ivan on 07.05.2017.
 */
public class App {
    private static String connStr;

    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        InputStream inputStream = null;

        try {
            inputStream = new FileInputStream("src/main/resources/config.properties");

            properties.load(inputStream);

            connStr = "Endpoint=" + properties.getProperty("EventHubCompatibleEndpoint") + ";" +
                    "EntityPath=" + properties.getProperty("EventHubCompatibleName") + ";" +
                    "SharedAccessKeyName=iothubowner" + ";" +
                    "SharedAccessKey=" + properties.getProperty("SharedAccessKey");

            EventHubClient client0 = receiveMessages("0");
            EventHubClient client1 = receiveMessages("1");
            System.out.println("Press ENTER to exit.");
            System.in.read();
            try {
                client0.closeSync();
                client1.closeSync();
                System.exit(0);
            } catch (ServiceBusException sbe) {
                System.exit(1);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static EventHubClient receiveMessages(final String partitionId)
    {
        EventHubClient client = null;

        try {
            client = EventHubClient.createFromConnectionStringSync(connStr);
        } catch(Exception e) {
            System.out.println("Failed to create client: " + e.getMessage());
            System.exit(1);
        }

        try {
            client.createReceiver(
                    EventHubClient.DEFAULT_CONSUMER_GROUP_NAME,
                    partitionId,
                    Instant.now()).thenAccept(receiver -> {
                        System.out.println("** Created receiver on partition " + partitionId);
                        try {
                            while (true) {
                                Iterable<EventData> receivedEvents = receiver.receive(100).get();
                                int batchSize = 0;
                                if (receivedEvents != null) {
                                    for(EventData receivedEvent: receivedEvents) {
                                        System.out.println(String.format("Offset: %s, SeqNo: %s, EnqueueTime: %s",
                                                receivedEvent.getSystemProperties().getOffset(),
                                                receivedEvent.getSystemProperties().getSequenceNumber(),
                                                receivedEvent.getSystemProperties().getEnqueuedTime()));
                                        System.out.println(String.format("| Device ID: %s", receivedEvent.getSystemProperties().get("iothub-connection-device-id")));
                                        System.out.println(String.format("| Message Payload: %s", new String(receivedEvent.getBody(),
                                                Charset.defaultCharset())));
                                        batchSize++;
                                    }
                                }
                                System.out.println(String.format("Partition: %s, ReceivedBatch Size: %s", partitionId,batchSize));
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to receive messages: " + e.getMessage());
                        }
                    });
        } catch (Exception e) {
            System.out.println("Failed to create receiver: " + e.getMessage());
        }

        return client;
    }
}
