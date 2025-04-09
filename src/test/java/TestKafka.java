import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * test kafka can't poll data
 */
public class TestKafka {

    String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    String topic = "TEST";

    /**
     * {@link #prepareData}
     */
    @Test
    public void testConsumer() {
        prepareData();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(getConsumerConfig())) {
            TopicPartition topicPartition = new TopicPartition(topic, 0);
            consumer.assign(List.of(topicPartition));

            consumer.seek(topicPartition, 3);
            // empty records
            Assertions.assertTrue(consumer.poll(Duration.ofSeconds(1)).records(topicPartition).isEmpty());

            consumer.seek(topicPartition, 5);
            // not empty
            Assertions.assertFalse(consumer.poll(Duration.ofSeconds(1)).records(topicPartition).isEmpty());

            consumer.seek(topicPartition, 11);
            // empty records
            Assertions.assertTrue(consumer.poll(Duration.ofSeconds(1)).records(topicPartition).isEmpty());
        }
    }

    /**
     * log.segment.bytes set to 50kb, msg set to 15kb, make every 3 msg will create new segment.
     * <br/>
     * prepare data:
     * <pre>
     *   ┌────────┬───────┬─────────┬───────┐
     *   │0 1 2   │4   6 7│8 9 10   │12     │
     *   └──────▲─┴──▲────┴───────▲─┴───▲───┘
     * box = segment, num = offset, ▲ = commitTransaction: non-continuous offsets appear
     * </pre>
     * <p>
     */
    public void prepareData() {
        try (Admin admin = Admin.create(getProducerConfig());
             KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(getProducerConfig())) {

            admin.deleteTopics(Collections.singletonList(topic));
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));

            producer.initTransactions();
            byte[] values = filledByteArray(15);
            // 1st segment
            sendTxMsg(producer, topic, values, 3);
            // 2nd segment
            sendTxMsg(producer, topic, values, 1);
            // 2nd & 3rd segments
            sendTxMsg(producer, topic, values, 5);
            // 4th segments
            sendTxMsg(producer, topic, values, 1);
        }
    }

    /**
     * Transactional message sending
     */
    public void sendTxMsg(KafkaProducer<byte[], byte[]> producer, String topic, byte[] value, int count) {
        producer.beginTransaction();
        for (int i = 0; i < count; i++) {
            producer.send(new ProducerRecord<>(topic, value));
        }
        producer.commitTransaction();
    }


    /**
     * Create a byte array of the specified size
     */
    public byte[] filledByteArray(int kb) {
        int totalBytes = kb * 1024;
        byte[] byteArray = new byte[totalBytes];
        byte fillValue = 0;
        Arrays.fill(byteArray, fillValue);
        return byteArray;
    }

    Properties getConsumerConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }


    Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client-1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 100);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-tx");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "2000");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        return props;
    }

}
