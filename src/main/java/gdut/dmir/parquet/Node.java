package gdut.dmir.parquet;

class Node {
	private long K;
    private String V;
    public Node() {
        super();
    }
    public Node(long K,String V) {
        //super();
        this.K = K;
        this.V = V;
    }
    public long getK() {
        return K;
    }
    public void setK(long K) {
        this.K = K;
    }
    public String getV() {
        return V;
    }
    public void setV(String V) {
        this.V = V;
    }
   
}