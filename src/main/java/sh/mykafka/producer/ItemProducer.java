package sh.mykafka.producer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import sh.mykafka.model.Item;
import sh.mykafka.partition.HashPartitioner;
import sh.mykafka.serializer.GenericSerializer;



public class ItemProducer {
	
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.81.130:9092");
		props.put("acks", "all");
		props.put("retries", 3);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", GenericSerializer.class.getName());
		props.put("value.serializer.type", Item.class.getName());
		props.put("partitioner.class", HashPartitioner.class.getName());

		Producer<String, Item> producer = new KafkaProducer<String, Item>(props);
		List<Item> items = readItem();
		items.forEach((Item item) -> producer.send(new ProducerRecord<String, Item>("items", item.getItemName(), item)));
		producer.close();
	}
	
	public static List<Item> readItem() throws IOException {
		List<String> lines = IOUtils.readLines(ItemProducer.class.getResourceAsStream("/items.csv"), Charset.forName("UTF-8"));
		List<Item> items = lines.stream()//创建爱你stream
			.filter(StringUtils::isNoneBlank)//过滤空行
			.map((String line) -> line.split("\\s*,\\s*"))//根据正则（空格，空格）分割成数组
			.filter((String[] values) -> values.length == 4)//过滤长度为4的数组
			.map((String[] values) -> new Item(values[0], values[1], values[2], Double.parseDouble(values[3])))//数组转换为item
			.collect(Collectors.toList());
		return items;
	}
	
}
