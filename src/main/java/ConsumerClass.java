import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class ConsumerClass {

    private static Consumer<String, String> createConsumer(String topicName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));
        return consumer;
    }


    private static void getMessageFromTopics(String name) {
        final Consumer<String, String> consumer = createConsumer(name);
        boolean isClosedConsumer = false;
        while (!isClosedConsumer) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            for (ConsumerRecord record : consumerRecords) {
                if (record.value().equals(Constants.EXIT_WORD)) {
                    consumer.commitAsync();
                    consumer.close();
                    isClosedConsumer = true;
                    System.out.println("The consuming is finishing");
                    continue;
                }
                System.out.printf("Consumer record: (from user:%s, message: %s, %s, %s)\n",
                        record.key(), record.value(), record.partition(), record.offset());
                consumer.commitAsync();
            }
        }
    }

    private static boolean isConsumerForPrivateMode(String key, String name) {
        String mode = key.split("-")[0];
        String consumerName = key.split("-")[1];
        if (Constants.PRIVATE_MODE.equals(mode) && name.equals(consumerName)) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Please, put your name!");
            String name = scanner.next();
            System.out.println("Please, choose a mode: bc for broadcast, pv for private!");
            String mode = scanner.next();
            if (Constants.BROADCAST_MODE.equals(mode)) {
                getMessageFromTopics(Constants.TOPIC);
            } else {
                getMessageFromTopics(name);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
