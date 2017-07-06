package sh.mykafka.model;

public class User {
	private String userName;
	private String userAddress;
	private String gender;
	private int age;

	public User() {}

	public User(String userName, String userAddress, String gender, int age) {
		this.userName = userName;
		this.userAddress = userAddress;
		this.gender = gender;
		this.age = age;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserAddress() {
		return userAddress;
	}

	public void setUserAddress(String userAddress) {
		this.userAddress = userAddress;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

}
