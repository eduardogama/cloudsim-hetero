package cloudproject.transcoding;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;





//SortingCloudletsBroker is inherited to DatacenterBroker
public class TranscodingBroker extends DatacenterBroker{
	
	/** The cloudlet new arrival list. */
	protected List<Binary> cloudletNewList = new ArrayList<Binary>();
	protected List<Binary> cloudletList = new ArrayList<Binary>();
	
	public static List <Binary> highestPriorityCloudletList = new ArrayList<Binary>();;

	//create sending cloudlet event
	public final static int CLOUDLET_SUBMIT_RESUME = 125;
   //exchange completion time between datacenter and broker	
	public final static int ESTIMATED_COMPLETION_TIME = 126;
   //create period event
	public final static int PERIODIC_EVENT = 127;
	
   //create period drop video event
	public final static int DROP_VIDEO = 128;
	
	//create a vm type event
	private static final int CREATE_VM_TYPE = 133;
	
   //video Id
	public static int videoId = 1;
	
    double periodicDelay = 5; //contains the delay to the next periodic event
    boolean generatePeriodicEvent = true; //true if new internal events have to be generated
    //boolean generatePeriodicEvent = false;
    
	//All the instance share cloudletNewArrivalQueue and cloudletBatchqueue, both of them are synchronized list
	private static List<Binary> cloudletNewArrivalQueue = Collections.synchronizedList(new ArrayList<Binary>());
	private static List<Binary> cloudletBatchQueue = Collections.synchronizedList(new ArrayList<Binary>());	
	
	public List<TranscodingVm> vmDestroyedList = new ArrayList<TranscodingVm>();

	//set size of local vm queue
	//private final static int waitinglist_max = 2;
	private int waitinglist_max;
	
	public Map<Integer, Double> totalCompletionTime_vmMap = new HashMap<Integer, Double>();
	public static Map<Integer, Double> totalCompletionTime_vmMap_Min = new HashMap<Integer, Double>();
	
	//Track the disply start up time
	private Map<Integer, Double> displayStartupTimeMap = new HashMap<Integer, Double>();
	private Map<Integer, Double> displayStartupTimeRealMap = new HashMap<Integer, Double>();
	
	private Map<Integer, Double> videoStartupTimeMap = new HashMap<Integer, Double>();
 
	private static int waitingListSize = 0;

    
	
    int vmIndex;
    int temp_key = 0;
    
    
    int cloudletSubmittedCount;
    boolean broker_vm_deallocation_flag =false;
    private static boolean dropVideoFlag = true;
	
	//vm Cost
    private static double vmCost = 0;
    
    //set up DatacenterCharacteristics;
	public DatacenterCharacteristics characteristics;
	public boolean startupqueue;
	public String sortalgorithm;
	public String schedulingmethod;
	public long rentingTime;
	
    //flag = 0, cloudlet is from batch queue
    //flag = 1, cloudlet is from new arrival queue
    private int switch_flag = 0;

	public TranscodingBroker(String name, DatacenterCharacteristics characteristics, String propertiesFileURL) throws Exception {
         super(name);
         
         Properties prop = new Properties();
		 //InputStream input = new FileInputStream("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudsim-impl/config.properties");
         InputStream input = new FileInputStream(propertiesFileURL);

         prop.load(input);
		
		 String waitinglist = prop.getProperty("waitinglist_max", "2");
		 String startupqueue = prop.getProperty("startupqueue", "true");
		 String rentingTime = prop.getProperty("rentingTime", "10000");
				 
		 this.sortalgorithm = prop.getProperty("sortalgorithm", "SDF");		 
		 this.waitinglist_max = Integer.valueOf(waitinglist);
		 this.startupqueue = Boolean.valueOf(startupqueue);
		 this.characteristics = characteristics;
		 this.schedulingmethod = prop.getProperty("schedulingmethod", "MMUT");
		 this.rentingTime = Long.valueOf(rentingTime);
		 
		 
		 
      // TODO Auto-generated constructor stub
    }
	
	
	public TranscodingBroker(String name) throws Exception{
		super(name);
	}
	

	/**
	 * Processes events available for this Broker.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {
		// Resource characteristics request
			case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
				processResourceCharacteristicsRequest(ev);
				break;
			case CloudSimTags.RESOURCE_CHARACTERISTICS:
				processResourceCharacteristics(ev);
				break;
			// VM Creation answer
			case CloudSimTags.VM_CREATE_ACK:
				processVmCreate(ev);
				break;
			// Resource characteristics answer
			// A finished cloudlet returned
			case CloudSimTags.CLOUDLET_RETURN:
				processCloudletReturn(ev);
				break;					
			// if the simulation finishes
			case CloudSimTags.END_OF_SIMULATION:
				shutdownEntity();
				break;
			/**
			* @override
			* add a new tag CLOUDLET_SUBMIT_RESUME to broker
			* updating cloudletwaitinglist in BinaryScheduler, whenever a vm's waitinglist is smaller
			* than 2, it will add a event in the broker, so that broker can keep send this vm the rest of 
			* cloudlet in its batch queue
			**/	
		    case CLOUDLET_SUBMIT_RESUME: 
		    	resumeSubmitCloudlets(ev); 
		    	break;
		    case ESTIMATED_COMPLETION_TIME:
		    	setVmCompletionTime(ev);
		    	//submitCloudlets();
		    	break;
		    // drop videos
			/*case DROP_VIDEO:
				dropVideo();
				break;*/

			// other unknown tags are processed by this method
			default:
				processOtherEvent(ev);
				break;
		}
	}

    /**
     * Calculate Vm cost 
     * @param ev
     */
	public void setVmCost(TranscodingVm vm){
		vmCost += vm.getCostPerSec()*(vm.getVmFinishTime() - vm.getStartTime())/1000.0;

	}
	
	/**
	 * get vm Cost;
	 * @param ev
	 */
	public double getVmCost(){
		return vmCost;
	}
	
    private void resumeSubmitCloudlets(SimEvent ev) {
	   submitCloudlets();
	 }
    
    private void setVmCompletionTime (SimEvent ev) {
        if (ev.getData() instanceof Map) {
        	totalCompletionTime_vmMap =(Map) ev.getData();
        }
    }
    
    private Map<Integer, Double> getVmCompletionTime(){
    	return totalCompletionTime_vmMap;
    }
    

    
    /**
	 * Process the ack received due to a request for VM creation.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	protected void processVmCreate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
		int vmId = data[1];
		int result = data[2];

		if (result == CloudSimTags.TRUE) {
			getVmsToDatacentersMap().put(vmId, datacenterId);
			getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
					+ " has been created in Datacenter #" + datacenterId + ", Host #"
					+ VmList.getById(getVmsCreatedList(), vmId).getHost().getId() + "\n");
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
					+ " failed in Datacenter #" + datacenterId);
		}
        
		incrementVmsAcks();
		
		
		

		// all the requested VMs have been created
		//if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
		
		List<Vm> vmTempList = new ArrayList<Vm>();
		vmTempList.addAll(getVmsCreatedList());
		vmTempList.removeAll(vmDestroyedList);
		if(vmTempList.size() > 0){
		  Map<Integer, Double> totalCompletionTime_vmMap_temp = new HashMap<Integer, Double>();
		  totalCompletionTime_vmMap_temp = totalCompletionTime_vmMap;
			//Initial all vm completion time as 0.
			 for(Vm vm: vmTempList){
				    if(totalCompletionTime_vmMap_temp.containsKey(vm.getId())){
				    	totalCompletionTime_vmMap.put(vm.getId(), totalCompletionTime_vmMap_temp.get(vm.getId()));
						//Log.printLine("Copying completion time map into temp!" + "\n");
				    }else{
				    	//System.out.println("\ninitial vmcompletiontimemap test");
		        	    totalCompletionTime_vmMap.put(vm.getId(), 0.0);
		        	    //Log.printLine("Init completion time map = 0 " + "\n");
				    }
		     }
			// Don't think it is doing anything here, since I am considering a static system, the cloudlets will be submitted only after all vms are created 
			//submitCloudlets();
			// Not so sure about the else part, consider this later maybe!
		} else {
			// all the acks received, but some VMs were not created
			if (getVmsRequested() == getVmsAcks()) {
				// find id of the next datacenter that has not been tried
				for (int nextDatacenterId : getDatacenterIdsList()) {
					if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
						createVmsInDatacenter(nextDatacenterId);
						return;
					}
				}

				// all datacenters already queried
				if (getVmsCreatedList().size() > 0) { // if some vm were created
					submitCloudlets();
				} else { // no vms created. abort
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": none of the required VMs could be created. Aborting");
					finishExecution();
				}
			}
		}
	}
	
	protected void killVM(Integer vmNum){
		TranscodingVm vm = (TranscodingVm) getVmList().get(vmNum);	
		if(!vmDestroyedList.contains(vm)){
				VideoSchedulerSpaceShared vmcsch = (VideoSchedulerSpaceShared) vm.getCloudletScheduler();

		    	if(vmcsch.getCloudletExecList().size() == 0 && vmcsch.getCloudletWaitingList().size() == 0 && vmcsch.getCloudletPausedList().size() == 0){
			    	System.out.println(CloudSim.clock() + "\n********************VM_" + vm.getId() + "'s renting time is over and to be destroyed from submit cloudlet***********************" );
		    		sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
	                vmDestroyedList.add(vm);
	                
	                //set Vm's finish time.
	                vm.setVmFinishTime(CloudSim.clock());
	    	        
	                //Calculate vm cost based on the time it last.
	                setVmCost(vm);
		        }
			}

	    	
	      totalCompletionTime_vmMap.remove(vmNum);		
		
	}
	
	protected void submitCloudlets() {

		
		/**
		 * Check if we can insert the first cloudlet in the new arrival queue to the front of the batch queue. 
		 * 1. calculate the minimum completion time when cloudlet_new insert in the front of batch queue
		 * 2. compare this minimum completion time to the deadline of the cloudlet in the front of batch queue
		 * 
		 */
		
		
		//cloudlet will be sent to vm
		Binary cloudlet;
		Binary cloudlet_new;
		Binary cloudlet_batch;
		//VideoStreams vstream = new VideoStreams();
		TranscodingVm vm;
		
		Map.Entry<Integer, Double> minCompletionTime_vm;
		TranscodingVm vmToSend;

	
		double estimated_completionTime;
		
		/* 1. calculate the minimum completion time when cloudlet_new insert in the front of batch queue */
		
		//Considering there is no startupqueue
			
			/*
			 * without new arrival queue
			 */

			
			//get the first cloudlet in the batch queue
			int total_elements = getCloudletList().size();
			if(total_elements > 0){
				
				/**
				 * 1. find the group of cloudlets that is the first GOP of each video
				 * 
				 */
				//for debug
				

				Binary vseg;
				/*
				for (Cloudlet cl:getCloudletList()){
					vseg = (Binary) cl;
					System.out.println(vseg.getCloudletSha1() + " for task = " + vseg.getCloudletTask_type());
				}
				*/
				int batch = Math.min(3, getCloudletList().size());

				for(int i=0;i<total_elements;i++){
					Cloudlet cl = getCloudletList().get(i);
					vseg = (Binary)cl;
					if (batch <0)
						break;
					//Maybe add logic to keep the size of highestPriorityList upto a max value
					if (!highestPriorityCloudletList.contains(vseg)){
						highestPriorityCloudletList.add(vseg);
						batch--;
					}
						
				}
				
				System.out.println("High priority List Contents");
				for (Binary b: highestPriorityCloudletList){
					System.out.println(b.getCloudletSha1() + " " + b.getCloudletTask_type());
				}
				
				Map<Binary, Integer> minCompletionTime_cloudletToVmMap = new HashMap<Binary, Integer>();
				Map<Binary, Double> minCompletionTime_cloudletToTimeMap = new HashMap<Binary, Double>();
				Map<Integer, Double> minCompletionTime_vmMap = new HashMap<Integer, Double>();
				
				//Map.Entry<String, Double> minMinCompletionTime = null;
				int minCompletionVmIndex = 0;
				double minCompletionTime = Double.MAX_VALUE;
				Binary minCompletionTimeCloudlet = null;
				List<Binary> newVideoList = new ArrayList<Binary>();
				
				double cloudletEstimatedCompletionTime;
				
				//if(schedulingmethod.equals("MM")){
					/**
					 * 2. implement original scheduling Method: MM, MSD, MMU
					 * 
					 */
					
					//a. Find the first available M
				
				//Code only for min-min
			  	
		   // if(schedulingmethod.equals("MM") || schedulingmethod.equals("MSD") || schedulingmethod.equals("MMU")){
					
			    for(Binary vsg:highestPriorityCloudletList){	
					
					    Map<Integer, Double> tempTotalCompletionTime_vmMap = new HashMap<Integer, Double>(totalCompletionTime_vmMap);
						System.out.println("Total Completion time VM MAp");
							for (Map.Entry<Integer, Double> entry : totalCompletionTime_vmMap.entrySet()) {
							    System.out.println(entry.getKey()+" : "+entry.getValue());
						}
					    
					    
						for(Integer vmNum:tempTotalCompletionTime_vmMap.keySet()){			
							
							if (vsg.getVmId() == -1) {
								
								
								vm = (TranscodingVm) getVmList().get(vmNum);
								boolean flag = vm.getDeallocationFlag();
								double rt = vm.getRemainingTime();
								//Check if this vm is about to be deallocated or renting time is over
								//kill vm
								if(vm.getDeallocationFlag() && vm.getRemainingTime() <= 0){
									killVM(vmNum);

							      continue;
								}else if(!vm.getDeallocationFlag() && vm.getRemainingTime() <= 0 ){
									if(vm.getPeriodicUtilizationRate() > 0.2){
								    	 vm.setRentingTime(rentingTime);
										 System.out.println("\n****** VM" +vm.getId() + " has bee recharged from submit cloudlet*****");
									}else{
										System.out.println("\n****** VM" +vm.getId() + " renting time expired and utilization is too low, therefore set deallocation flag and remove it*****");
										vm.setDeallocationFlag(true);	
										killVM(vmNum);
								    	continue;
									}
								 
							   	}
		
							} else { // submit to the specific vm
								vm = (TranscodingVm) VmList.getById(getVmList(), vsg.getVmId());
								if (vm == null) { 
									// vm was not created
									Log.printLine(getName() + ": Postponing execution of cloudlet "
											+ vsg.getCloudletId() + ": bount VM not available");
									return;
								}
							}	
							//Set Each VM CloudletWaitingList to 2, so if a VM's waitingList is beyound 2, current cloudlet 
							//won't be sent to specific vm.
							

					    	cloudletEstimatedCompletionTime = getEstimatedCompletionTime(vsg, vm, vmNum);
					    	System.out.println("Estimated completion time for " + vsg.getCloudletSha1() + " for task " + vsg.getCloudletTask_type() + " on " + vm.getId() + " " + vm.getVmType() + " is " + cloudletEstimatedCompletionTime );
					    	
					    	minCompletionTime_vmMap.put(vmNum, cloudletEstimatedCompletionTime);
					    	
					    	

					     }//End of vm loop, WE have expected completion time on each vm in the system(minCompletionTime_vmMap) for the given cloudlet
					  
						 //After calculate each cloudlet's completion time on every VM, find and store the minimum completion time VM to that cloudlet	
						 minCompletionTime_vm = null;
					     for (Map.Entry<Integer, Double> entry : minCompletionTime_vmMap.entrySet()) {
						      if (minCompletionTime_vm == null || minCompletionTime_vm.getValue() >= entry.getValue()) {
						           minCompletionTime_vm = entry;
						      }
						 }
					     if(minCompletionTime_vm == null){
					    	   vmIndex = 0;
					     }else {
					           vmIndex = minCompletionTime_vm.getKey();
					     }			   
					  
					   minCompletionTime_cloudletToVmMap.put(vsg, vmIndex);
					   minCompletionTime_cloudletToTimeMap.put(vsg, minCompletionTime_vm.getValue());
				 }
				
							
							 
						 
					  // b.After we find the first M for each cloudlet, we begin to find the second M for MM
					  
					if(schedulingmethod.equals("MM")){
						//Find the minimum completion time cloudlet and VM# 
						for(Binary vsg:highestPriorityCloudletList){
								 													
							 if(minCompletionTime_cloudletToTimeMap.get(vsg) < minCompletionTime) {		
								 
								 minCompletionTime = minCompletionTime_cloudletToTimeMap.get(vsg);
								 minCompletionTimeCloudlet = vsg;
								 minCompletionVmIndex = minCompletionTime_cloudletToVmMap.get(vsg);	
								 
							 }
								 					
						}
					}
				//}

			 cloudlet = minCompletionTimeCloudlet;
		   
				
	      }
			else{
				Log.printLine("Sorry no cloudlets present here!");
				return;
		      }
				
		
			
			
		// if user didn't bind this cloudlet and it has not been executed yet
		if (cloudlet.getVmId() == -1) {
			//vm = getVmsCreatedList().get(vmIndex);
			vm = (TranscodingVm) getVmList().get(vmIndex);

		} else { // submit to the specific vm
			vm = (TranscodingVm) VmList.getById(getVmList(), cloudlet.getVmId());
			if (vm == null) { 
				
				Log.printLine(getName() + ": Postponing execution of cloudlet "
						+ cloudlet.getCloudletId() + ": bount VM not available");
				return;
			}
		}	
		//Set Each VM CloudletWaitingList to 2, so if a VM's waitingList is beyound 2, current cloudlet 
		//won't be sent to specific vm.
		VideoSchedulerSpaceShared vcsch = (VideoSchedulerSpaceShared) vm.getCloudletScheduler();
		List<? extends ResCloudlet> waitinglist = vcsch.getCloudletWaitingList();
		
		if (waitinglist.size() >= waitinglist_max) {
			return;
		} else {
	
			
			Log.printLine(CloudSim.clock() + getName() + ": Sending Video ID: " + cloudlet.getCloudletSha1() + " Cloudlet "
						+ cloudlet.getCloudletId() + " to VM #" + vm.getId());
			cloudlet.setVmId(vm.getId());
			
	        EventData data = new EventData(cloudlet, totalCompletionTime_vmMap);
	        
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, data);
			cloudletsSubmitted++;
					
			getCloudletSubmittedList().add(cloudlet);	
			
			highestPriorityCloudletList.remove(cloudlet);
					
			//remove this cloudlet from cloudlet list
			cloudlet.setSegmentExecutedVmType(vm.getVmType());
			getCloudletList().remove(cloudlet);
			

		}
      
	}
	
	public double getEstimatedCompletionTime(Binary vsg, TranscodingVm vm, int vmNum){
		
		double cloudletEstimatedCompletionTime;
		
		
		//Set Each VM CloudletWaitingList to 2, so if a VM's waitingList is beyound 2, current cloudlet 
		//won't be sent to specific vm.
		VideoSchedulerSpaceShared vcschTemp = (VideoSchedulerSpaceShared) vm.getCloudletScheduler();
		double capacity = 0.0;
		int cpus = 0;
		for (Double mips : vcschTemp.getCurrentMipsShare()) {
			capacity += vm.getMips();
			if (mips > 0) {
				cpus++;
			}
		}

		//int currentCpus = cpus;
		capacity /= cpus;
		
    	//check vm type
        String vmType = vm.getVmType(); 
       
    	
    	long cloudletLengthVm = vsg.getCloudletLengthMap().get(vmType);
    	vsg.setCloudletLength(cloudletLengthVm);
		
    	//cloudletEstimatedCompletionTime = vsg.getCloudletLength() / capacity + totalCompletionTime_vmMap.get(vmNum) + CloudSim.clock();
    	cloudletEstimatedCompletionTime = cloudletLengthVm / capacity + totalCompletionTime_vmMap.get(vmNum) + CloudSim.clock();

    	return cloudletEstimatedCompletionTime;
    	
	}
	
	protected void processCloudletReturn(SimEvent ev) {
		//Check if submitted cloudlet was packed and add unpacked binary'es tasks to the batch queue
		Binary cloudlet = (Binary) ev.getData();
		getCloudletReceivedList().add(cloudlet);
		
		Log.printLine(CloudSim.clock() + getName() + " : Video Id" + cloudlet.getCloudletSha1() + " Cloudlet " + cloudlet.getCloudletId()
				+ " finished in VM" + cloudlet.getVmId());
		cloudletsSubmitted--;
		
		
		
		//copy cloudletSubmitted for Coordinator use.
		cloudletSubmittedCount = cloudletsSubmitted;
		
		//Get this finished cloudlet's vm Id
		TranscodingVm vm = (TranscodingVm) VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
		
        //reset local queue completion time map		
		double totalCompletionTime_temp =0;
		int vmId = cloudlet.getVmId();
		double execTime = cloudlet.getActualCPUTime();
		if(!vm.getDeallocationFlag() || vm.getRemainingTime() > 0){	

			//calculate the difference between the real exec time and estimated exec time
			double estimatedExecTime = cloudlet.getCloudletLength() / vm.getMips();
			double execTime_dif = execTime - estimatedExecTime;

			
			totalCompletionTime_temp = totalCompletionTime_vmMap.get(vmId) - execTime + execTime_dif;
			totalCompletionTime_vmMap.put(vmId, totalCompletionTime_temp);		
		}
		
		

        
		//Check if this vm is set to be destroyed
		if(vm.getDeallocationFlag() && vm.getRemainingTime() <= 0) {	
			killVM(vm.getId());

		}else if(!vm.getDeallocationFlag() && vm.getRemainingTime() <= 0 ){
			if(vm.getPeriodicUtilizationRate() > 0.2){
		    	 vm.setRentingTime(rentingTime);
				 System.out.println("\n****** VM" +vm.getId() + " has been recharged from cloudlet return*****");
			}else{
				

				
		        System.out.println("\n****** VM" +vm.getId() + " renting time expired and utilization is too low, therefore set deallocation flag and remove it from cloudlet return*****");
				
				vm.setDeallocationFlag(true);	
				killVM(vm.getId());

			}
	   	}
		
		//Cleaning up just encountered last cloudlet
		
		if (getCloudletList().size() == 0 && getCloudletNewList().size() == 0 && cloudletsSubmitted == 0 && !generatePeriodicEvent) { // all cloudlets executed
			//Log.printLine(CloudSim.clock() + ": " + getName() + ": All Cloudlets executed. Finishing...");
			Log.printLine(CloudSim.clock() + ": " + getName() + ": All Cloudlets executed. Finishing...");
            getVmsCreatedList().removeAll(vmDestroyedList);
			clearDatacenters();
			finishExecution();
		} else { // some cloudlets haven't finished yet
			
			if (getCloudletList().size() > 0 || getCloudletNewList().size() > 0) {
				submitCloudlets();
			}
			// Not sure about this one though!	
			if (getCloudletList().size() > 0 && getCloudletNewList().size() > 0 && cloudletsSubmitted == 0) {
				// all the cloudlets sent finished. It means that some bount
				// cloudlet is waiting its VM be created 
				clearDatacenters();
				createVmsInDatacenter(0);
			}

		}
		
	}
	
	
	/**
	 * This method is used to send to the broker the list of cloudlets.
	 * 
	 * @param list the list
	 * @pre list !=null
	 * @post $none
	 */
	public void submitCloudletList(List<? extends Cloudlet> cloudletBatchQueue, List<? extends Cloudlet> cloudletNewArrivalQueue) {
		//Before submit new cloudlet list, delete those who have already been submitted. 

	    List<Cloudlet> cloudletBatchQueue_temp = Collections.synchronizedList(new ArrayList<Cloudlet>());	
	    List<Cloudlet> cloudletNewQueue_temp = Collections.synchronizedList(new ArrayList<Cloudlet>());	
	   
	    cloudletBatchQueue_temp.addAll(cloudletBatchQueue);
	    cloudletNewQueue_temp.addAll(cloudletNewArrivalQueue);
	    
		//Delete Cloudlets which are already submitted
	    for(Cloudlet cl:cloudletBatchQueue_temp) {
			 if (getCloudletSubmittedList().contains(cl)){
				 cloudletBatchQueue.remove(cl);
			 }			 
		}
		
		//Delete duplicated cloudlets which are already in the batch queue.
		for(Cloudlet cl:cloudletBatchQueue_temp) {
			 if (getCloudletList().contains(cl)){
				 cloudletBatchQueue.remove(cl);
			 }			 
		}
		
		for(Cloudlet cl:cloudletNewQueue_temp) {
			 if (getCloudletSubmittedList().contains(cl)){
				 cloudletNewArrivalQueue.remove(cl);
			 }			 
		}
		
		//Main part here
		//Simply adding new cloudlets to the batch queue
		getCloudletList().addAll(cloudletBatchQueue);
		getCloudletNewList().addAll(cloudletNewArrivalQueue);
		ArrayList<Integer> newcloudlets = new ArrayList<Integer>();
		
		for(int i=0; i < cloudletNewList.size(); i++){
			 newcloudlets.add(cloudletNewList.get(i).getCloudletId());

		}
		
		
   	    System.out.println(Thread.currentThread().getName() + "*****New arrival queue Video ID_" + videoId + ": " + newcloudlets + " **********");

   	    System.out.println("**********************The size of batch queue is: " + getCloudletList().size() + " **************");

	}
	/**
	 * Gets the cloudlet batch queue list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Cloudlet> List<T> getCloudletList() {
		return (List<T>) cloudletList;
	}
	
	
	/**
	 * Gets the cloudlet new arrival list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Cloudlet> List<T> getCloudletNewList() {
		return (List<T>) cloudletNewList;
	}
	
	
	public Map<Integer, Double> getVideoStartupTimeMap() {
		return videoStartupTimeMap;
	}
	
	
	public void clearDatacenters() {
		for (Vm vm : getVmsCreatedList()) {
			Log.printConcatLine(CloudSim.clock(), ": " + getName(), ": Destroying VM #", vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
			
			
			//set Vm's finish time.
	        TranscodingVm vmm = (TranscodingVm)vm;
	        vmm.setVmFinishTime(CloudSim.clock());
	        
	        //Calculate vm cost based on the time it last.
	        setVmCost(vmm);
		}

		getVmsCreatedList().clear();
		
		
		
	}
	
	
	
	/**
	 * Gets the vms to datacenters map.
	 * 
	 * @return the vms to datacenters map
	 */
	public Map<Integer, Integer> getVmsToDatacentersMap() {
		return vmsToDatacentersMap;
	}
	
	
	/**
	 * Send an internal event communicating the end of the simulation.
	 * 
	 * @pre $none
	 * @post $none
	 */
	public void finishExecution() {
		sendNow(getId(), CloudSimTags.END_OF_SIMULATION);
	}
	
	//Generate random number for random length cloudlets
	
	private static long getRandomNumber(int aStart, int aEnd, Random aRandom){
	    if (aStart > aEnd) {
	      throw new IllegalArgumentException("Start cannot exceed End.");
	    }
	    //get the range, casting to long to avoid overflow problems
	    long range = (long)aEnd - (long)aStart + 1;
	    // compute a fraction of the range, 0 <= frac < range
	    long fraction = (long)(range * aRandom.nextDouble());
	    long randomNumber =  (long)(fraction + aStart); 
	    
	    return randomNumber;
	}	

/*	
	//sorted by cloudlet length
		protected void FCFS() {
			List<Binary> lstCloudlets = getCloudletList();
			List<Binary> lstNewCloudlets = getCloudletNewList();
			
			for (int a = 0; a < lstCloudlets.size(); a++) {
	            for (int b = a + 1; b < lstCloudlets.size(); b++) {
	            	
	            	if(lstCloudlets.get(b).getArrivalTime() < lstCloudlets.get(a).getArrivalTime()){
                		Binary temp = lstCloudlets.get(a);
	                    lstCloudlets.set(a, lstCloudlets.get(b));
	                    lstCloudlets.set(b, temp);
                	}
	            }
	        }
			setCloudletList(lstCloudlets);
			
		}
		
		
		//sorted by cloudlet deadline
		protected void SortedbyDeadline() {
			List<Binary> lstCloudlets = getCloudletList();
			List<Binary> lstNewCloudlets = getCloudletNewList();
			
			for (int a = 0; a < lstCloudlets.size(); a++) {
	            for (int b = a + 1; b < lstCloudlets.size(); b++) {
	                if (lstCloudlets.get(b).getCloudletDeadline() < lstCloudlets.get(a).getCloudletDeadline()) {
	                    Binary temp = lstCloudlets.get(a);
	                    lstCloudlets.set(a, lstCloudlets.get(b));
	                    lstCloudlets.set(b, temp);
	                }
	            }
	        }
			
			setCloudletList(lstCloudlets);

		}
		
	    //sorted by cloudlet's estimated shortest execution time (SJF)
		protected void SortedbySJF() {
			List<Binary> lstCloudlets = getCloudletList();
			//lstCloudlets= getCloudletList();
			//setCloudletList(lstCloudlets);
			int reqTasks=lstCloudlets.size();
			int reqVms=vmList.size();
			ArrayList<Double> executionTimeList = new ArrayList<Double>();
	      //  System.out.println("\n\t PRIORITY  Broker Schedules\n");
	       // System.out.println("Before ordering");

	          for (int i=0;i<reqTasks;i++)
	          {
	            executionTimeList.add(( lstCloudlets.get(i).getCloudletLength())/ (lstCloudlets.get(i).getNumberOfPes() * vmList.get(i%reqVms).getMips()) );
	          //  System.out.println("CLOUDLET ID" + " " +lstCloudlets.get(i).getCloudletId() +" EXE TIME   " +  executionTimeList.get(i));

	          }
	             for(int i=0;i<reqTasks;i++)
	                    {
	                    for (int j=i+1;j<reqTasks;j++)
	                            {
	                            if (executionTimeList.get(i) > executionTimeList.get(j))
	                                {

	                                    Binary temp1 = lstCloudlets.get(i);
	                                    lstCloudlets.set(i, lstCloudlets.get(j));
	                                    lstCloudlets.set(j, temp1);    
	                                    
	                                    double temp2 = executionTimeList.get(i);
	                                    executionTimeList.set(i, executionTimeList.get(j));
	                                    executionTimeList.set(j, temp2);

	                            }

	                            }
	                    }

	         setCloudletList(lstCloudlets);			
		}
*/
		
		/**
		 * Create VMs
		 * @param userId
		 * @param vms
		 * @return
		 */
	 
		public List<TranscodingVm> createVM(int userId, String vmType, int vms, int idShift, long rentingTime) {

			//Creates a container to store VMs. This list is passed to the broker later
			LinkedList<TranscodingVm> list = new LinkedList<TranscodingVm>();
			
			InstanceType it = new InstanceType(vmType);
			
			long size = it.getInstanceStorageSize();
			int ram = it.getInstanceRam();
			int mips = it.getInstanceMips();
			long bw = it.getInstanceBw();
			int pesNumber = it.getInstancePesNumber();
			double costPerSec = it.getInstanceCost();
			String vmm = "Xeon";
			double periodicUtilizationRate = 0.0;
			
			
			//memCost += characteristics.getCostPerMem()*ram;
			
			//Whenver ther is a Vm created, add storage cost.
			//storageCost += characteristics.getCostPerStorage()*size/1024;

			//create VMs
			TranscodingVm[] vm = new TranscodingVm[vms];

			for(int i=0;i<vms;i++){
				//vm[i] = new Vm(i, userId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
				//for creating a VM with a space shared scheduling policy for cloudlets:
				
				//always run vm#0
				if(idShift == 0){
					
					vm[i] = new TranscodingVm(i+idShift, userId, mips, pesNumber, ram, bw, size, Long.MAX_VALUE, costPerSec, periodicUtilizationRate, vmType, vmm, new VideoSchedulerSpaceShared());
	                System.out.println("A " + vmType + " EC2 instance has been created....");
	                
				}else{
				
			    	vm[i] = new TranscodingVm(i+idShift, userId, mips, pesNumber, ram, bw, size, rentingTime, costPerSec, periodicUtilizationRate, vmType, vmm, new VideoSchedulerSpaceShared());
	                System.out.println("A " + vmType + " EC2 instance has been created....");
	            
				}
				list.add(vm[i]);
			}

			return list;
		}
		
		
		/*
		 * Destroy VM event
		 */
	   public void destroyVmEvent(TranscodingVm vmm){
		   sendNow(getVmsToDatacentersMap().get(vmm.getId()), CloudSimTags.VM_DESTROY, vmm);
           vmDestroyedList.add(vmm);
           
           //set Vm's finish time.
           vmm.setVmFinishTime(CloudSim.clock());
	        
           //Calculate vm cost based on the time it last.
           setVmCost(vmm);
		   
	   }
	   
	   /**
	    * Create VM event
	    * 
	    */
	   public void createVmEvent(TranscodingVm vmm){
		 //submit it to broker
			submitVmList(vmm);
			
			//creat a event for datacenter to create a vm
	    	sendNow(getVmsToDatacentersMap().get(vmm.getId()),CloudSimTags.VM_CREATE_ACK, vmm);
	   }
	   
	   public void submitVmList(TranscodingVm vmm) {
			getVmList().add(vmm);
		}
	   
	   

	   /**
	    * Get priority queue	
	    * @return
	    */
	   public List<Binary> getHighestPriorityCloudletList() {
			return highestPriorityCloudletList;
		}
	   

	   /**
	    * Set priority queue	
	    * @return
	    */
		public void setHighestPriorityCloudletList(
				List<Binary> highestPriorityCloudletList) {
			this.highestPriorityCloudletList = highestPriorityCloudletList;
		}
	  
		
		
		

}
