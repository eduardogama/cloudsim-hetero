package cloudproject.transcoding;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;

public class Transformation {
	
	private long rentingTime;
	private TranscodingBroker broker;
	private TranscodingDatacenter datacenter;
	
	public Transformation(TranscodingBroker broker, TranscodingDatacenter datacenter){
		this.broker = broker;
		this.datacenter = datacenter;
	}
	
    public Transformation(String propertiesFileURL) throws IOException{
    	Properties prop = new Properties();
		InputStream input = new FileInputStream(propertiesFileURL);
		prop.load(input);
		
		String rentingTime;
		
		rentingTime = prop.getProperty("rentingTime", "60000");
		this.rentingTime = Long.valueOf(rentingTime);
		
	}
	
	public void transfromVms(){
		//when a vm's remaining time is lessing than 0, either recharge it or remove it based it's deallocation flag 
		int runningVmNum = 0;	
		double vmUtilizationRate;
		
		TranscodingProvisioner tper;
		
		
		
	    int allcoationDeallocationFlag = 0;
	
		tper = new TranscodingProvisioner();									
		allcoationDeallocationFlag = tper.allocateDeallocateVm();
		
		
		for(Vm vm : datacenter.getVmList()){
			TranscodingVm vmm = (TranscodingVm) vm;
			VideoSchedulerSpaceShared vmcsc = (VideoSchedulerSpaceShared) vmm.getCloudletScheduler();
	        System.out.println("*********The remaining time of VM" + vmm.getId() + " is " + vmm.getRemainingTime() + "******");
			
	        
			if(vmm.getRemainingTime() >= 10){
				runningVmNum ++;
			}else if(vmm.getRemainingTime()<= 10 && !vmm.getDeallocationFlag()){
				
				vmUtilizationRate = vmm.getPeriodicUtilizationRate();
				
				if(vmUtilizationRate > 0.2){
				    vmm.setRentingTime(rentingTime);
				    System.out.println("     ****** VM" +vmm.getId() + " has been recharged from resource provisiong *****");
				}else if(vmUtilizationRate <= 0.2 && allcoationDeallocationFlag == 1){
					   
					    TranscodingProvisioner tp;
					    double weight;
						double highestWeight = 0.0;
					    String vmType = null;
					    double alpha = 0.8;
					    
					   
						 tp = new TranscodingProvisioner();
						 
						 Map<String, Double> missedDeadlineGopTypeMap = new HashMap<String, Double>(tp.getMissedDeadlineGopTypeMap());
						 Map<String, Double> batchGopTypeMap = new HashMap<String, Double>(tp.getBatchGopTypeMap());
						 
						 
						 for(String key:batchGopTypeMap.keySet()){
							    
							   weight = alpha*missedDeadlineGopTypeMap.get(key) + (1-alpha)*batchGopTypeMap.get(key);
							   
							   if(weight > highestWeight){
								   highestWeight = weight;
								   vmType = key;
							   }
							   
						  }		 								 

					    
					    if( highestWeight > 0.1){
					        System.out.println("\n****** VM" +vmm.getId() + " renting time expired and utilization is too low, therefore transform if to high needs VM type: " + vmType + " from resource provision*****");
	                        provisionVM(1, vmType); 
					    }else{
	
				            System.out.println("\n****** VM" +vmm.getId() + " renting time expired and utilization is too low, therefore set deallocation flag and remove it from resource provision*****");
					    }
					    
		              //  System.out.println("     ****** VM" +vmm.getId() + " renting time expired and utilization is too low, therefore set deallocation flag and remove it from resource provision*****");
	
						
						vmm.setDeallocationFlag(true);	
						
						VideoSchedulerSpaceShared scheduler = (VideoSchedulerSpaceShared)vmm.getCloudletScheduler();
				    	if(scheduler.getCloudletExecList().size() == 0 && scheduler.getCloudletWaitingList().size() == 0 && scheduler.getCloudletPausedList().size() == 0) {
				    		
				    		System.out.println(CloudSim.clock() + "      ********************Cloudles in VM_" + vmm.getId() + " have finished from resource provisioning***********************" );
				    		broker.destroyVmEvent(vmm);
				    	}
         
			    }
	
			}else if(vmm.getRemainingTime() <= 0 && vmm.getDeallocationFlag() && vmcsc.getCloudletExecList().size() == 0 && vmcsc.getCloudletWaitingList().size() == 0 && vmcsc.getCloudletPausedList().size() == 0){
			    	//System.out.println(CloudSim.clock() + "\n********************VM_" + vmm.getId() + "'s renting time is over and to be destroyed***********************" );
					broker.destroyVmEvent(vmm);
			}
		}
	}
	
	
	public double getHeterogeneouRate(){
		int totalVmTypeNum = 5;
		int totalVmNum = 0;
		double heterogeneousDiversityRate = 0.0;
		int vmNum = 0;
		double sum = 0.0;
		double H;
		double Hmax;
		double E;
		Map<String, Integer> vmTypeNumMap = new HashMap<String, Integer>();
		List<Double> pi = new ArrayList<Double>();
		
		for(Vm vm : datacenter.getVmList()){
			TranscodingVm vmm = (TranscodingVm) vm;
			if(!vmTypeNumMap.containsKey(vmm.getVmType())){
				totalVmNum ++;
				vmTypeNumMap.put(vmm.getVmType(), 1);
			}else{
				totalVmNum ++;
				vmNum = vmTypeNumMap.get(vmm.getVmType()) + 1;
				vmTypeNumMap.put(vmm.getVmType(), vmNum);
			}
		}
		
		
		
		for(String key:vmTypeNumMap.keySet()){
			double p = vmTypeNumMap.get(key)*1.0/totalVmNum;
			pi.add(p);
		}
		
		for(int i=0; i<pi.size(); i++){
			sum += pi.get(i)*Math.log(pi.get(i));
		}
		
		H = 0-sum;
		Hmax = Math.log(totalVmTypeNum);
		E = H/Hmax;
		
		
		return E;	
		
	}
	
	/**
	 * Allocate VMs based on Startup Queue Length
	 */
	public void provisionVM(long vmNum, String vmType){
		
		if(vmType == null){
			vmType = "c4.xlarge";
		}else{
		
		//	System.out.println("\n**creating "+ vmNum + " " + vmType + " vms...\n");
            int vmIdShift = broker.getVmsCreatedList().size();
			for(int i=0; i<vmNum; i++){
			    List<TranscodingVm> vmNew = (List<TranscodingVm>) broker.createVM(broker.getId(), vmType, 1, vmIdShift, rentingTime);
			    TranscodingVm vmm = vmNew.get(0);
			    
                vmIdShift++;	            
	            broker.createVmEvent(vmm);
			}
		}
		
	}

}
