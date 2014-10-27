public class SelectResult {
	public String id_str;
    public String created_at;
    public String text;
    public String coor1;
    public String coor2;
    
    public SelectResult(String id_str){
    	this.id_str = id_str;
    }
    
    public void setTime(String created_at){
    	this.created_at = created_at;
    }
    
    public void setCoor1(String c1) {
    	this.coor1 = c1;
    }
    
    public void setCoor2(String c2) {
    	this.coor2 = c2;
    }
    
    public void setText(String text) {
    	this.text = text;
    }
	@Override
	public String toString() {
		return "SelectResult [id_str=" + id_str + ", created_at=" + created_at
				+ ", text=" + text + ", coor1=" + coor1 + ", coor2=" + coor2
				+ "]";
	}
    
    
}