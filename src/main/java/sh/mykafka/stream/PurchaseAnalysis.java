package sh.mykafka.stream;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;


import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import sh.mykafka.model.Item;
import sh.mykafka.model.Order;
import sh.mykafka.model.User;
import sh.mykafka.serializer.SerdesFactory;
import sh.mykafka.timeextractor.OrderTimestampExtractor;

public class PurchaseAnalysis {

	public static void main(String[] args) throws IOException, InterruptedException {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-purchase-analysis");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.81.130:9092");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "192.168.81.130:2181/kafka");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimestampExtractor.class);

		KStreamBuilder streamBuilder = new KStreamBuilder();
		KStream<String, Order> orderStream = streamBuilder.stream(Serdes.String(), SerdesFactory.serdFrom(Order.class), "orders");
		//定义ktable的用户详细：支持用户详情的更新和增加
		KTable<String, User> userTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(User.class), "users", "users-state-store");
		//定义ktable的商品详细：支持商品详情的更新和增加
		KTable<String, Item> itemTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(Item.class), "items", "items-state-store");
		
//			KTable<KeyCategoryItem, ValueOrders> kTable = orderStream
		//订单左连接用户信息：加入年龄信息
		orderStream.leftJoin(userTable, (Order order, User user) -> OrderUser.fromOrderUser(order, user), Serdes.String(), SerdesFactory.serdFrom(Order.class))
					//过滤年龄18到35岁
					.filter((String userName, OrderUser orderUser) -> orderUser.getAge() >=18 && orderUser.getAge() <=35)
					//调整key和value为订单商品名和商品数量
					.map((String userName, OrderUser orderUser) -> new KeyValue<String, String>(orderUser.getItemName(), String.valueOf(orderUser.getQuantity())))
					//根据商品名分组计算分组商品数量总和，使用hopping window：interval为5秒，window size为1小时
					.groupByKey().aggregate(
							() -> 0,
							(itemName, quantity, aggregate) -> aggregate + Integer.valueOf(quantity), 
							TimeWindows.of(1 * 60 * 60 * 1000).advanceBy(5 * 1000),
							Serdes.Integer(), 
							"quantities").toStream()
							//调整key和value为订单商品名和聚合后的结果包含窗口信息
							.map((Windowed<String> window, Integer value) -> new KeyValue<String, OrderWindow>(window.key(), OrderWindow.fromOrderWindow(window,value)))
					//左连接item：加入商品类别信息和商品总价
					.leftJoin(itemTable, (OrderWindow orderWindow, Item item) -> OrderItem.fromOrderItem(orderWindow, item), Serdes.String(), SerdesFactory.serdFrom(OrderWindow.class))
					//过滤数量为0的记录
					.filter((String itemName, OrderItem orderItem) -> orderItem.getQuantity() > 0)
					//调整key为商量类别，value为窗口统计结果，并输出到topic result
					.map((String itemName, OrderItem orderItem) -> new KeyValue<String, String>(orderItem.getCategory(), orderItem.toString()))
					//输出结果到topic result
					.to("result");
		
		KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, props);
		kafkaStreams.cleanUp();
		kafkaStreams.start();
		
		System.in.read();
		kafkaStreams.close();
		kafkaStreams.cleanUp();
	}
	
	public static class OrderUser {
		private String userName;
		private String itemName;
		private long transactionTs;
		private int quantity;
		
		private int age;
		
		
		public String getItemName() {
			return itemName;
		}
		public void setItemName(String itemName) {
			this.itemName = itemName;
		}
		public long getTransactionTs() {
			return transactionTs;
		}
		public void setTransactionTs(long transactionTs) {
			this.transactionTs = transactionTs;
		}
		public int getQuantity() {
			return quantity;
		}
		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}
		
		public String getUserName() {
			return userName;
		}
		public void setUserName(String userName) {
			this.userName = userName;
		}
		public int getAge() {
			return age;
		}
		public void setAge(int age) {
			this.age = age;
		}
		public static OrderUser fromOrderUser(Order order, User user){
			OrderUser orderUser = new OrderUser();
			orderUser.setItemName(order.getItemName());
			orderUser.setQuantity(order.getQuantity());
			orderUser.setTransactionTs(order.getTransactionTs());
			orderUser.setUserName(order.getUserName());
			orderUser.setAge(user.getAge());
			return orderUser;
		}
		
	}
	
	public static class OrderWindow{
		private String itemName;
		private int quantities;
		private long windowStart;
		private long windowEnd;
		public String getItemName() {
			return itemName;
		}
		public void setItemName(String itemName) {
			this.itemName = itemName;
		}
		public int getQuantities() {
			return quantities;
		}
		public void setQuantities(int quantities) {
			this.quantities = quantities;
		}
		public long getWindowStart() {
			return windowStart;
		}
		public void setWindowStart(long windowStart) {
			this.windowStart = windowStart;
		}
		
		public long getWindowEnd() {
			return windowEnd;
		}
		public void setWindowEnd(long windowEnd) {
			this.windowEnd = windowEnd;
		}
		
		public static OrderWindow fromOrderWindow(Windowed<String> window, Integer value){
			OrderWindow orderWindow = new OrderWindow();
			orderWindow.setItemName(window.key());
			orderWindow.setQuantities(value);
			orderWindow.setWindowStart(window.window().start());;
			orderWindow.setWindowEnd(window.window().end());
			return orderWindow;
		}
		@Override
		public String toString() {
			return "OrderWindow [itemName=" + itemName + ", quantities=" + quantities + ", windowStart=" + windowStart
					+ ", windowEnd=" + windowEnd + "]";
		}
		
	}
	
	
	public static class OrderItem {
		private String itemName;
		private int quantity;
		private long windowStart;
		private long windowEnd;
		
		private String category;
		private double price;
		
		private double total;
		
		
		public String getItemName() {
			return itemName;
		}
		public void setItemName(String itemName) {
			this.itemName = itemName;
		}
		public int getQuantity() {
			return quantity;
		}
		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}
		public String getCategory() {
			return category;
		}
		public void setCategory(String category) {
			this.category = category;
		}
		public double getPrice() {
			return price;
		}
		public void setPrice(double price) {
			this.price = price;
		}
		
		public double getTotal() {
			return total;
		}
		public void setTotal(double total) {
			this.total = total;
		}
		
		public long getWindowStart() {
			return windowStart;
		}
		public void setWindowStart(long windowStart) {
			this.windowStart = windowStart;
		}
		public long getWindowEnd() {
			return windowEnd;
		}
		public void setWindowEnd(long windowEnd) {
			this.windowEnd = windowEnd;
		}
		
		public static OrderItem fromOrderItem(OrderWindow orderWindow, Item item){
			OrderItem orderItem = new OrderItem();
			orderItem.setItemName(orderWindow.getItemName());
			orderItem.setQuantity(orderWindow.getQuantities());
			orderItem.setWindowStart(orderWindow.getWindowStart());
			orderItem.setWindowEnd(orderWindow.getWindowEnd());
			orderItem.setCategory(item.getCategory());
			orderItem.setPrice(item.getPrice());
			orderItem.setTotal(orderWindow.getQuantities() * item.getPrice());
			return orderItem;
		}
		
		public OrderItem merge(OrderItem orderItem){
			this.setQuantity(this.getQuantity() + orderItem.getQuantity());
			this.setTotal(this.getTotal() + orderItem.getTotal());
			return orderItem;
		}
		@Override
		public String toString() {
			return "OrderItem [itemName=" + itemName + ", quantity=" + quantity
					+ ", windowStart=" + windowStart + ", windowEnd="
					+ windowEnd + ", category=" + category + ", price=" + price
					+ ", total=" + total + "]";
		}
		
	}
	
	
	public static class KeyCategoryItem{
		private String category;
		private String itemName;
		
		public KeyCategoryItem(){
			
		}
		public KeyCategoryItem(String category, String itemName) {
			this.category = category;
			this.itemName = itemName;
		}
		public String getCategory() {
			return category;
		}
		public void setCategory(String category) {
			this.category = category;
		}
		public String getItemName() {
			return itemName;
		}
		public void setItemName(String itemName) {
			this.itemName = itemName;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((category == null) ? 0 : category.hashCode());
			result = prime * result
					+ ((itemName == null) ? 0 : itemName.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			KeyCategoryItem other = (KeyCategoryItem) obj;
			if (category == null) {
				if (other.category != null)
					return false;
			} else if (!category.equals(other.category))
				return false;
			if (itemName == null) {
				if (other.itemName != null)
					return false;
			} else if (!itemName.equals(other.itemName))
				return false;
			return true;
		}
		@Override
		public String toString() {
			return "KeyCategoryItem [category=" + category + ", itemName="
					+ itemName + "]";
		}
	}
	
	public static class ValueOrders{
		private int quantity;
		private double price;
		private double total;
		private int order;
		
		public ValueOrders(){
			
		}

		public ValueOrders(int quantity, double price, int order) {
			super();
			this.quantity = quantity;
			this.price = price;
			this.total = quantity * price;
			this.order = order;
		}

		public int getQuantity() {
			return quantity;
		}

		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}

		public double getPrice() {
			return price;
		}

		public void setPrice(double price) {
			this.price = price;
		}

		public double getTotal() {
			return total;
		}

		public void setTotal(double total) {
			this.total = total;
		}

		public int getOrder() {
			return order;
		}

		public void setOrder(int order) {
			this.order = order;
		}

		@Override
		public String toString() {
			return "ValueOrders [quantity=" + quantity + ", price=" + price
					+ ", total=" + total + ", order=" + order + "]";
		}
		
		public ValueOrders merge(ValueOrders v){
			this.setQuantity(this.getQuantity() + v.getQuantity());
			this.setTotal(this.getTotal() + v.getTotal());
			return this;
		}
	}
}
