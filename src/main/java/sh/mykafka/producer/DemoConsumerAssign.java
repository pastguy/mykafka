package sh.mykafka.producer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import sh.mykafka.model.Order;

public class DemoConsumerAssign {

	public static void main(String[] args) {
		args = new String[] { "192.168.81.130:9092", "result01", "group11", "consumer11" };
		// args = new String[] { "192.168.179.131:9092",
		// "orderuser-repartition-by-item", "group1", "consumer3" };
		if (args == null || args.length != 4) {
			System.err.println(
					"Usage:\n\tjava -jar kafka_consumer.jar ${bootstrap_server} ${topic_name} ${group_name} ${client_id}");
			System.exit(1);
		}
		String bootstrap = args[0];
		String topic = args[1];
		String groupid = args[2];
		String clientid = args[3];

		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrap);
		props.put("group.id", groupid);
		props.put("client.id", clientid);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		// props.put("value.deserializer", GenericDeserializer.class.getName());

		props.put("auto.offset.reset", "earliest");
		KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
		consumer.assign(Arrays.asList(new TopicPartition(topic, 0)));
		while (true) {
			ConsumerRecords<String, Order> records = consumer.poll(100);
			records.forEach(record -> {
				String msg = String.format("client : %s , topic: %s , partition: %d , offset = %d, key = %s, value = %s, timestamp = %d %n",
						clientid, record.topic(), record.partition(), record.offset(), record.key(), record.value(),record.timestamp()) ;
				System.out.printf("client : %s , topic: %s , partition: %d , offset = %d, key = %s, value = %s, timestamp = %d %n",
						clientid, record.topic(), record.partition(), record.offset(), record.key(), record.value(),record.timestamp());
				writeFile(msg);
			});
		}
	}

	public static void writeFile(String msg) {
		try {
			File file = new File("d:/result.txt");
			FileOutputStream in = new FileOutputStream(file,true);
			in.write(msg.getBytes(), 0, msg.getBytes().length);
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
