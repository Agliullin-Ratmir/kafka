import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class ProducerClass {

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static void sendMessage(String key, String message) throws Exception {
        final Producer<String, String> producer = createProducer();

        try {
                final ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(Constants.TOPIC, key, message);
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("sent record(key=%s, value = %s), meta (partition=%s, offset=%s)\n",
                        record.key(), record.value(), metadata.partition(), metadata.offset());

        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String mode = null;
        String message = null;
        try {
            while (!Constants.EXIT_WORD.equals(message)) {
                System.out.println("Please, choose a mode: bc for broadcast, pv for private!");
                mode = scanner.next();
                System.out.println("Please, put your message!");
                message = scanner.next();
                if (Constants.BROADCAST_MODE.equals(mode)) {
                    sendMessage(Constants.BROADCAST_MODE, message);
                } else {
                    System.out.println("Please, put a consumer's name!");
                    String name = scanner.next();
                    sendMessage(Constants.PRIVATE_MODE + "-" + name, message);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
