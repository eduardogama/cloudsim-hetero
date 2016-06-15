package cloudproject.transcoding;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

public class TranscodingVm extends Vm{
	
	private double startTime;
	private double remainingTime;
	private double rentingTime;
	private boolean toBeDeallocated;
	private double vmFinishTime;
    private String vmType;
    private double costPerSec;
    private double periodicUtilizationRate;
	
	public TranscodingVm(int id, 
			             int userId, 
			             double mips, 
			             int numberOfPes,
			             int ram, 
			             long bw, 
			             long size, 
			             long rentingTime,
			             double costPerSec,
			             double periodicUtilizationRate,
			             String vmType,
			             String vmm,
			             CloudletScheduler cloudletScheduler) {
		super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
		this.rentingTime = rentingTime + CloudSim.clock();
		this.vmType = vmType;
		this.costPerSec = costPerSec;
		this.periodicUtilizationRate = periodicUtilizationRate;
		setStartTime(CloudSim.clock());
		//setRentingTime(60000 + CloudSim.clock());
 		setDeallocationFlag(false);
 		//setRentingTime(100000);
		
		// TODO Auto-generated constructor stub
	}
	
	
	
	public double getPeriodicUtilizationRate() {
		return periodicUtilizationRate;
	}



	public void setPeriodicUtilizationRate(double periodicUtilizationRate) {
		this.periodicUtilizationRate = periodicUtilizationRate;
	}



	public String getVmType(){
		return vmType;
	}
	
	public double getCostPerSec(){
		return costPerSec;
	}
	
	/**
	 * get Vm start up time	
	 * @return
	 */
    public double getStartTime() {
    	return startTime;
    }
    
    /**
     * set vm start up time
     * @param startTime
     */
    public void setStartTime(double startTime) {
    	this.startTime = startTime;
    }
    
    /**
     * get vm remaining time
     * @return
     */
    public double getRemainingTime() {
    //	this.remainingTime = rentingTime + CloudSim.clock() - startTime;
    	return rentingTime - CloudSim.clock();
    }
    
  /*  public void setRemainingTime(double remainintTime) {
    	this.remainingTime = remainingTime; 
    }*/
    
    /**
     * set vm renting time
     * @param rentingTime
     */
    public void setRentingTime(double rentingTime){
    	this.rentingTime = rentingTime + CloudSim.clock();
    }
    
    
    public double getRentingTime(){
    	return rentingTime;
    }
    /**
     * get vm Deallocation signal, to see if this vm will be deallocated or not.
     * @return
     */
    
    public boolean getDeallocationFlag(){
    	return toBeDeallocated;
    }
    /**
     * set vm deallocation signal
     * @param toBeDeallocatedFlag
     */
    
    public void setDeallocationFlag(boolean toBeDeallocatedFlag){
    	this.toBeDeallocated = toBeDeallocatedFlag;
    }
    
    /**
     * get Vm finish time
     */
    
    public double getVmFinishTime(){
    	return vmFinishTime;
    }
    
    /**
     * Set vm finish time
     */
    
    public void setVmFinishTime(double vmFinishTime){
    	this.vmFinishTime = vmFinishTime;
    }
    
    
    
    
    
}
