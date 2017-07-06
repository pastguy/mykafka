package sh.mykafka.model;

public class Order {

	private String userName;
	private String itemName;
	private long transactionTs;
	private int quantity;

	public Order() {}

	public Order(String userName, String itemName, long transactionTs,
			int quantity) {
		this.userName = userName;
		this.itemName = itemName;
		this.transactionTs = transactionTs;
		this.quantity = quantity;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

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
	
}
