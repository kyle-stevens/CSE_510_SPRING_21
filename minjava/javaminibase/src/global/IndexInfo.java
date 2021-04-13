package global;

public class IndexInfo {

	private String relationName;
	private int indexType;
	private int attr_num;
	private int clustered;
	private int hash1;
	private int numBuckets;
	private int splitPointer;
	public String getRelationName() {
		return relationName;
	}
	public void setRelationName(String relationName) {
		this.relationName = relationName;
	}
	public int getIndexType() {
		return indexType;
	}
	public void setIndexType(int indexType) {
		this.indexType = indexType;
	}
	public int getAttr_num() {
		return attr_num;
	}
	public void setAttr_num(int attr_num) {
		this.attr_num = attr_num;
	}
	public int getClustered() {
		return clustered;
	}
	public void setClustered(int clustered) {
		this.clustered = clustered;
	}
	public int getHash1() {
		return hash1;
	}
	public void setHash1(int hash1) {
		this.hash1 = hash1;
	}
	public int getNumBuckets() {
		return numBuckets;
	}
	public void setNumBuckets(int numBuckets) {
		this.numBuckets = numBuckets;
	}
	public int getSplitPointer() {
		return splitPointer;
	}
	public void setSplitPointer(int splitPointer) {
		this.splitPointer = splitPointer;
	}
	
	
}
