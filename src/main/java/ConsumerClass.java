import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class ConsumerClass {

    private static Consumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(Constants.TOPIC));
        return consumer;
    }

    private static void getMessage(String name) throws Exception {
        final Consumer<String, String> consumer = createConsumer();
        boolean isClosedConsumer = false;
            while (!isClosedConsumer) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            for(ConsumerRecord record : consumerRecords) {
                if (record.value().equals("exit")) {
                    consumer.commitAsync();
                    consumer.close();
                    isClosedConsumer = true;
                    System.out.println("The consuming is finishing");
                    continue;
                }
                if (Constants.BROADCAST_MODE.equals(record.key()) || isConsumerForPrivateMode(String.valueOf(record.key()), name)) {
                    consumer.commitAsync();
                    System.out.printf("Consumer record: (%s, %s, %s, %s)\n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
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
            getMessage(name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
