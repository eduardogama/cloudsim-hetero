package cloudproject.transcoding;

public class TestCreateVM {
	
	public static void main(String args[]){
		TranscodingMain ts = new TranscodingMain();
		ts.createVM(0, "c4.large", 1, 0, Long.MAX_VALUE);
	}

}
