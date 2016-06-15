package cloudproject.transcoding;

public class InstanceType {
	
	private String instanceType = null;
	private int instanceMips;
	private int instanceRam;
	private long instanceBw;
	private int instancePesNumber;
	private long instanceStorageSize; 
	private double instanceCost;

	public InstanceType(){
		
	}
	
	public InstanceType(String instanceType){
		this.instanceType = instanceType;
		
	}

	public int getInstanceMips() {
		
		if(instanceType.equalsIgnoreCase("c4.xlarge")){
	    	instanceMips = 8000;
		}else if (instanceType.equalsIgnoreCase("r3.xlarge")){
	    	instanceMips = 6000;
		}else if (instanceType.equalsIgnoreCase("t2.small")){
	    	instanceMips = 1100;
		}else if (instanceType.equalsIgnoreCase("g2.2xlarge")){
	    	instanceMips = 13000;
		}else if (instanceType.equalsIgnoreCase("m4.large")){
	    	instanceMips = 4000;
		}else{
			instanceMips = 8000;
		}
			
		return instanceMips;
		
	}
	
	public int getInstanceRam() {
		
		if(instanceType.equalsIgnoreCase("c4.xlarge")){ 
		    	instanceRam = 1024;
		}else if (instanceType.equalsIgnoreCase("r3.xlarge")){
		    	instanceRam = 1024;
		}else if (instanceType.equalsIgnoreCase("t2.small")){
		    	instanceRam = 1024;
		}else if (instanceType.equalsIgnoreCase("g2.2xlarge")){
		    	instanceRam = 1024;
		}else if (instanceType.equalsIgnoreCase("m4.large")){
	    	    instanceRam = 1024;
	    }else{
		    	instanceRam = 1024;
	    }
		return instanceRam;
	}


	public long getInstanceBw() {
		
		if(instanceType.equalsIgnoreCase("c4.xlarge")){ 
		    	instanceBw = 1000;
		}else if (instanceType.equalsIgnoreCase("r3.xlarge")){
		    	instanceBw = 1000;
		}else if (instanceType.equalsIgnoreCase("t2.small")){
		    	instanceBw = 1000;
		}else if (instanceType.equalsIgnoreCase("g2.2xlarge")){
		    	instanceBw = 1000;
		}else if (instanceType.equalsIgnoreCase("m4.large")){
	    	    instanceBw = 1000;
	    }else{
		    	instanceBw = 1000;
	    }
		return instanceBw;
	}

	
	public int getInstancePesNumber() {
		
		if(instanceType.equalsIgnoreCase("c4.xlarge")){ 
		    	instancePesNumber = 1;
		}else if (instanceType.equalsIgnoreCase("r3.xlarge")){
		    	instancePesNumber = 1;
		}else if (instanceType.equalsIgnoreCase("t2.small")){
		    	instancePesNumber = 1;
		}else if (instanceType.equalsIgnoreCase("g2.2xlarge")){
		    	instancePesNumber = 1;
		}else if (instanceType.equalsIgnoreCase("m4.large")){
	    	    instancePesNumber = 1;
	    }else{
		    	instancePesNumber = 1;
        }	
		return instancePesNumber;
	}
	
	public long getInstanceStorageSize() {
		
		if(instanceType.equalsIgnoreCase("c4.xlarge")){ 
		    	instanceStorageSize = 30720;
		}else if (instanceType.equalsIgnoreCase("r3.xlarge")){
		    	instanceStorageSize = 30720;
		}else if (instanceType.equalsIgnoreCase("t2.small")){
		    	instanceStorageSize = 30720;
		}else if (instanceType.equalsIgnoreCase("g2.2xlarge")){
		    	instanceStorageSize = 30720;
		}else if (instanceType.equalsIgnoreCase("m4.large")){
	    	    instanceStorageSize = 30720;
	    }else{
		    	instanceStorageSize = 30720;
	    }	
		
		return instanceStorageSize;
	}

	
	public double getInstanceCost(){
		
		if(instanceType.equalsIgnoreCase("c4.xlarge")){ 
		    	instanceCost = 0.209/3600;
		}else if (instanceType.equalsIgnoreCase("r3.xlarge")){
		    	instanceCost = 0.33/3600;
		}else if (instanceType.equalsIgnoreCase("t2.small")){
		    	instanceCost = 0.026/3600;
		}else if (instanceType.equalsIgnoreCase("g2.2xlarge")){
		    	instanceCost = 0.65/3600;
		}else if (instanceType.equalsIgnoreCase("m4.large")){
	    	    instanceCost = 0.12/3600;
	    }else{
		    	instanceCost = 0.209/3600;
	    }	
		
		return instanceCost;
	}
	
 
	
}
