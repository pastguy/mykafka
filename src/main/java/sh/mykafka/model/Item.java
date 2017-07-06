package sh.mykafka.model;

public class Item {
	private String itemName;
	private String itemAddress;
	private String category;
	private double price;

	public Item() {}

	public Item(String itemName, String itemAddress, String category,
			double price) {
		this.itemName = itemName;
		this.itemAddress = itemAddress;
		this.category = category;
		this.price = price;
	}

	public String getItemName() {
		return itemName;
	}

	public void setItemName(String itemName) {
		this.itemName = itemName;
	}

	public String getItemAddress() {
		return itemAddress;
	}

	public void setItemAddress(String itemAddress) {
		this.itemAddress = itemAddress;
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
	

}
