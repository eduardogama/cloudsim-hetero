package cloudproject.transcoding;

public class MapInstanceToGopLength {
	
	private long gopLength;
	
	public MapInstanceToGopLength(){
		
	}
	
	public Long getGopLength(String ec2Type, Long transcodingTime){
		
		//Based on the EC2 types, the MIPS are different, map different GOP length based on intances' MIPs
		if(ec2Type.equalsIgnoreCase("c4.xlarge")){ 
	    	gopLength = transcodingTime * 8000;
		}else if (ec2Type.equalsIgnoreCase("r3.xlarge")){
	    	gopLength = transcodingTime * 6000;
		}else if (ec2Type.equalsIgnoreCase("t2.small")){
	    	gopLength = transcodingTime * 1100;
		}else if (ec2Type.equalsIgnoreCase("g2.2xlarge")){
	    	gopLength = transcodingTime * 13000;
		}else if (ec2Type.equalsIgnoreCase("m4.large")){
	    	gopLength = transcodingTime * 4000;
		}else{
	    	gopLength = transcodingTime * 8000;
	    }
		
		return gopLength;
	}
	

}
