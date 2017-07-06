package sh.mykafka.timeextractor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import sh.mykafka.model.Item;
import sh.mykafka.model.Order;
import sh.mykafka.model.User;

import com.fasterxml.jackson.databind.JsonNode;

public class OrderTimestampExtractor implements TimestampExtractor {

	@Override
	public long extract(ConsumerRecord<Object, Object> record) {
		Object value = record.value();
		if (record.value() instanceof Order) {
			Order order = (Order) value;
			return order.getTransactionTs();
		}
		if (value instanceof JsonNode) {
			return ((JsonNode) record.value()).get("transactionTs").longValue();
		}
		if (value instanceof Item) {
			return LocalDateTime.of(2015, 12,11,1,0,10).toEpochSecond(ZoneOffset.UTC) * 1000;
		}
		if (value instanceof User) {
			return LocalDateTime.of(2015, 12,11,0,0,10).toEpochSecond(ZoneOffset.UTC) * 1000;
		}
//		return LocalDateTime.of(2015, 11,10,0,0,10).toEpochSecond(ZoneOffset.UTC) * 1000;
		return record.timestamp();
//		throw new IllegalArgumentException("OrderTimestampExtractor cannot recognize the record value " + record.value());
	}

}
