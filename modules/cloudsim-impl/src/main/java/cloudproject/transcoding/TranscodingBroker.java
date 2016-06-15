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
	protected List<VideoSegment> cloudletNewList = new ArrayList<VideoSegment>();
	protected List<VideoSegment> cloudletList = new ArrayList<VideoSegment>();
	
	public static List <VideoSegment> highestPriorityCloudletList = new ArrayList<VideoSegment>();;

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
	
	//All the instance share cloudletNewArrivalQueue and cloudletBatchqueue, both of them are synchronized list
	private static List<VideoSegment> cloudletNewArrivalQueue = Collections.synchronizedList(new ArrayList<VideoSegment>());
	private static List<VideoSegment> cloudletBatchQueue = Collections.synchronizedList(new ArrayList<VideoSegment>());	
	
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
			// Resource characteristics answer
			case CloudSimTags.RESOURCE_CHARACTERISTICS:
				processResourceCharacteristics(ev);
				break;
			// VM Creation answer
			case CloudSimTags.VM_CREATE_ACK:
				processVmCreate(ev);
				break;
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
			* updating cloudletwaitinglist in VideoSegmentScheduler, whenever a vm's waitinglist is smaller
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
	//	vmCost += characteristics.getCostPerSecond()*(vm.getVmFinishTime() - vm.getStartTime())/1000.0;
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
     * Drop videos
     */
    /*private void dropVideo(){
    	
    	List<Integer> videoDropList = new ArrayList<Integer>();
    	
    	//
    	for(int i=0; i<getCloudletList().size(); i++){
    		VideoSegment vs = (VideoSegment) getCloudletList().get(i);
    		if(vs.getOrderNum() == 0 && vs.getUtilityNum() <= 0.2){
    			if(!videoDropList.contains(vs.getCloudletVideoId())){
    				
        			videoDropList.add(vs.getCloudletVideoId());
        		//	System.out.println("Video #" + vs.getCloudletVideoId() + " is about to be dropped");
    			}else{
    				continue;
    			}   			
    		}
    	}
    	
    	for(int i=0; i<getCloudletList().size(); i++){
    		VideoSegment vst = (VideoSegment) getCloudletList().get(i);
    		
            if(videoDropList.contains(vst.getCloudletVideoId())){
            	
            	getCloudletList().remove(vst);
            	i--;
    		//	System.out.println("Video #" + vst.getCloudletVideoId() + " Cloudlet #" + vst.getCloudletId() + " has been dropped");

            	
            }else{
            	continue;
            }   		
    	}
    	
    	//Gaussian dissdribution of droping videos
    	Random r = new Random();
    	double eventDelay = r.nextGaussian() + 1000;
    	
    	if(getCloudletList().size() > 0 || generatePeriodicEvent){
    	   send(getId(), eventDelay, DROP_VIDEO);
    	}
    	
    }*/
    
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
				    }else{
				    	//System.out.println("\ninitial vmcompletiontimemap test");
		        	    totalCompletionTime_vmMap.put(vm.getId(), 0.0);
				    }
		     }
			submitCloudlets();
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
	
	protected void submitCloudlets() {
		
		/*if(dropVideoFlag && getCloudletList().size() > 0){
			dropVideo();
			dropVideoFlag = false;
		}*/
		
		/*if(sortalgorithm.equals("FCFS")){
		    FCFS();
		}else if(sortalgorithm.equals("SJF")){
			SortedbySJF();	
		}else if(sortalgorithm.equals("SDF")){
		    SortedbyDeadline();
		}*/
		
       /* if(CloudSim.clock() >= 120000){
        	System.out.println("Test broker..");
        }*/
		
		
		/**
		 * Check if we can insert the first cloudlet in the new arrival queue to the front of the batch queue. 
		 * 1. calculate the minimum completion time when cloudlet_new insert in the front of batch queue
		 * 2. compare this minimum completion time to the deadline of the cloudlet in the front of batch queue
		 * 
		 */
		
		
		//cloudlet will be sent to vm
		VideoSegment cloudlet;
		VideoSegment cloudlet_new;
		VideoSegment cloudlet_batch;
		//VideoStreams vstream = new VideoStreams();
		TranscodingVm vm;
		
		Map.Entry<Integer, Double> minCompletionTime_vm;
		TranscodingVm vmToSend;

	
		double estimated_completionTime;
		
		/* 1. calculate the minimum completion time when cloudlet_new insert in the front of batch queue */

		if(startupqueue){
		    /** 
	        * Now what it should do next:
	        * 1. find a calculate each vm's estimated completion time
	        * 2. find the smallest completion time vm, and send it cloudlet. 
	        */
			
			while(true){ 
									
				//
				//totalCompletionTime_vmMap_Min.putAll(totalCompletionTime_vmMap);
				
				minCompletionTime_vm = null;
				for (Map.Entry<Integer, Double> entry : totalCompletionTime_vmMap.entrySet()) {
				    if (minCompletionTime_vm == null || minCompletionTime_vm.getValue() >= entry.getValue()) {
				        minCompletionTime_vm = entry;
				    }
				}
				if(minCompletionTime_vm == null){
					vmIndex = 0;
				}else {
			        vmIndex = minCompletionTime_vm.getKey();
				}
				
	           
				
				
				
				//find the minimal completion time vmId
			   // vmToSend = (TranscodingVm) getVmsCreatedList().get(vmIndex);
				vmToSend = (TranscodingVm) getVmList().get(vmIndex);
				
			    
			    //check if this vm is about to be destroyed or not, if yes, find another one
				//check vm's remaining time
			    if(vmToSend.getDeallocationFlag() && vmToSend.getRemainingTime() <= 0 ) {
			    	
					VideoSchedulerSpaceShared vmcsch = (VideoSchedulerSpaceShared) vmToSend.getCloudletScheduler();

			    	
			    	if(vmcsch.getCloudletExecList() == null && vmcsch.getCloudletWaitingList() == null && vmcsch.getCloudletPausedList() == null){
				    	//System.out.println(CloudSim.clock() + "\n********************VM_" + vmToSend.getId() + "'s renting time is over and to be destroyed***********************" );
			    		sendNow(getVmsToDatacentersMap().get(vmToSend.getId()), CloudSimTags.VM_DESTROY, vmToSend);
		                vmDestroyedList.add(vmToSend);
		                
		                //set Vm's finish time.
		                vmToSend.setVmFinishTime(CloudSim.clock());
		    	        
		                //Calculate vm cost based on the time it last.
		                setVmCost(vmToSend);
			    	}
			    	
			    	totalCompletionTime_vmMap.remove(vmIndex);
			    	
			    	continue;
			  /*  }else if(!vmToSend.getDeallocationFlag() && vmToSend.getRemainingTime() <= 0 ){
			    	 vmToSend.setRentingTime(rentingTime);
					 System.out.println("\n****** VM" +vmToSend.getId() + " has bee recharged *****");
			        break;*/
			    
			    }else if(vmToSend.getDeallocationFlag() && vmToSend.getRemainingTime() > 0 ){
			    	/*System.out.println("\nThe VM#: " + vmToSend.getId() +" has been set to destroy, but the remaing time is: " + vmToSend.getRemainingTime() + " So "
			    			+ "keep sending cloudlets to this vm");*/
			    	
			    	break;
			    }else{
			    	break;
			    }
		    }  

			/**
			 * With new arrival queue
			 */
			//get the first cloudlet in the new arrival queue
			if(getCloudletNewList().size() > 0) {
				cloudlet_new = (VideoSegment) getCloudletNewList().get(0);

				
					//get the first cloudlet in the batch queue
					if(getCloudletList().size() > 0){
					    cloudlet_batch = (VideoSegment) getCloudletList().get(0);
					    //check if cloudlet in new arrial and batch are the same video stream
					    //If they are the same video, always send the new arrival queue first   
					    
					   // calculate the expected time for cloudlet completion
						if (cloudlet_new.getVmId() == -1) {
							//vm = getVmsCreatedList().get(vmIndex);
							vm = (TranscodingVm) getVmList().get(vmIndex);
	
						} else { // submit to the specific vm
							vm = (TranscodingVm) VmList.getById(getVmList(), cloudlet_new.getVmId());
							if (vm == null) { 
								// vm was not created
								/*Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
										+ cloudlet.getCloudletId() + ": bount VM not available");*/
								Log.printLine(getName() + ": Postponing execution of cloudlet "
										+ cloudlet_new.getCloudletId() + ": bount VM not available");
								return;
							}
						}	
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
				
						int currentCpus = cpus;
						capacity /= cpus;
						
				    	//check vm type
                        String vmType = vm.getVmType();
						
						
						
						//reset each cloudlet's deadline after the first cloudlet begins to display
					    if(cloudlet_new.getCloudletVideoId() == cloudlet_batch.getCloudletVideoId()) {
						    
					    	cloudlet =(VideoSegment) getCloudletNewList().get(0);
					      //  getCloudletNewList().remove(0);
					      //  vstream.getNewArrivalQueue().remove(0);
	
					    	switch_flag = 1; 
	                        
					    	/**
					    	 * Calculate random smaple from normal distribution
					    	 */
					    	Random r = new Random();
					    	double val = r.nextGaussian()*cloudlet_new.getCloudletStd() + cloudlet_new.getAvgCloudletLength();
					    	long sampleLength = (long) Math.round(val);
					    	
					    	long cloudletLengthVm = cloudlet_new.getCloudletLengthMap().get(vmType);
					    	cloudlet_new.setCloudletLength(cloudletLengthVm);
		    	
						    estimated_completionTime = cloudlet_new.getCloudletLength() / capacity + minCompletionTime_vm.getValue();
	                        
						    if(!displayStartupTimeMap.containsKey(cloudlet.getCloudletVideoId()) || cloudlet.getCloudletId() == 0){	
						         displayStartupTimeMap.put(cloudlet.getCloudletVideoId(), estimated_completionTime + CloudSim.clock());
								// cloudlet.setCloudletDeadline(minToCompleteTime);
							
								 for(VideoSegment vs:cloudletNewList){
									 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
										double test =  vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId());
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }
								 for(VideoSegment vs:cloudletList){
									 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }	 
							}
					    	
					    }else{//if they are not the same video, then continue algorithm
					
							
					
						
			/*			    *//** 
					        * Now what it should do next:
					        * 1. find a calculate each vm's estimated completion time
					        * 2. find the smallest completion time vm, and send it cloudlet. 
					        */
					    	
					    	/**
					    	 * Calculate random smaple from normal distribution
					    	 */
					    	Random r = new Random();
					    	double val = r.nextGaussian()*cloudlet_new.getCloudletStd() + cloudlet_new.getAvgCloudletLength();
					    	long sampleLength = (long) Math.round(val);
					    	
					    	long cloudletLengthVm = cloudlet_new.getCloudletLengthMap().get(vmType);
					    	cloudlet_new.setCloudletLength(cloudletLengthVm);
							
						    estimated_completionTime = cloudlet_new.getCloudletLength() / capacity + minCompletionTime_vm.getValue();
						    
						    //After calclulate the estimated min completion time of inserted new cloudlet, replace that vm's completion time
						    //Create a new map to find the minimum completion time Vm
						    
						    
						    Map<Integer, Double> totalCompletionTime_vmMap_New = new HashMap<Integer, Double>();
			                
						    totalCompletionTime_vmMap_New.putAll(totalCompletionTime_vmMap);
						    
						    for(Integer key:totalCompletionTime_vmMap_New.keySet()){
								if(key == vmIndex) {
									totalCompletionTime_vmMap_New.put(key,estimated_completionTime);
			
								}
							}
						    //compare this min completion time with other vm's completion time again in the totalCompletionTime_vmMap
			                //Find the min completion time VM afater insert new arrival cloudlet
						    
						    
						    Map.Entry<Integer, Double> minCompletionTime_vm_new = null;
							for (Map.Entry<Integer, Double> entry : totalCompletionTime_vmMap_New.entrySet()) {
							    if (minCompletionTime_vm_new == null || minCompletionTime_vm_new.getValue() > entry.getValue()) {
							        minCompletionTime_vm_new = entry;
							    }
							}
						    
						    //double minToCompleteTime =  minCompletionTime_vm_new.getValue() + cloudlet_batch.getCloudletLength() / capacity + CloudSim.clock();
						    double minToCompleteTime =  minCompletionTime_vm_new.getValue() + cloudlet_batch.getCloudletLengthMap().get(vmType) / capacity + CloudSim.clock();
	
						   // double cloudletDeadlineAbs = cloudlet_batch.getCloudletDeadline() + cloudlet_batch.getSubmissionTime(getId());
						    double cloudletDeadlineAbs = cloudlet_batch.getCloudletDeadline();
	
						    
						    //for test
							/**
							 * reset each cloudlet's deadline after the first cloudlet begins to display
							 * It's based on estimated finish time
							 */
	
					    	if(cloudletDeadlineAbs < minToCompleteTime){
						    	cloudlet = cloudlet_batch;
									switch_flag = 0;
								}else {
									cloudlet = cloudlet_new;
									
							       // getCloudletNewList().remove(0);
							      //  vstream.getNewArrivalQueue().remove(0);
	
									switch_flag = 1;
									
									
									if(!displayStartupTimeMap.containsKey(cloudlet.getCloudletVideoId()) || cloudlet.getCloudletId() == 0){	
								         displayStartupTimeMap.put(cloudlet.getCloudletVideoId(), estimated_completionTime + CloudSim.clock());
										// cloudlet.setCloudletDeadline(minToCompleteTime);
									
	
										 for(VideoSegment vs:cloudletNewList){
											 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
											    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
											 }
										 }
										 for(VideoSegment vs:cloudletList){
											 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
											    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
											 }
										 }	 
									}
									
									
						     }	
						    
						    
			           }   
					    
					    
					    
				//If batch queue is empty, but new arrival queue is not, send the new arrival cloudlet	
					}else {
					       cloudlet =(VideoSegment) getCloudletNewList().get(0);
					       switch_flag = 1;
					       
					       /**
					        * reset cloudlet deadline
					        */
					       // calculate the expected time for cloudlet completion
							if (cloudlet_new.getVmId() == -1) {
								//vm = getVmsCreatedList().get(vmIndex);
								vm = (TranscodingVm) getVmList().get(vmIndex);
	
							} else { // submit to the specific vm
								vm = (TranscodingVm) VmList.getById(getVmList(), cloudlet_new.getVmId());
								if (vm == null) { 
									// vm was not created
									/*Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
											+ cloudlet.getCloudletId() + ": bount VM not available");*/
									Log.printLine(getName() + ": Postponing execution of cloudlet "
											+ cloudlet_new.getCloudletId() + ": bount VM not available");
									return;
								}
							}	
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
					
							int currentCpus = cpus;
							capacity /= cpus;
					        cloudlet =(VideoSegment) getCloudletNewList().get(0);
					      //  getCloudletNewList().remove(0);
					      //  vstream.getNewArrivalQueue().remove(0);
					        
					    	switch_flag = 1; 
					    	
					    	//check vm type
	                        String vmType = vm.getVmType();
					    	
					    	/**
					    	 * Calculate random smaple from normal distribution
					    	 */
					    	Random r = new Random();
					    	double val = r.nextGaussian()*cloudlet_new.getCloudletStd() + cloudlet_new.getAvgCloudletLength();
					    	long sampleLength = (long) Math.round(val);

					    	long cloudletLengthVm = cloudlet_new.getCloudletLengthMap().get(vmType);
					    	cloudlet_new.setCloudletLength(cloudletLengthVm);
					    	
						    estimated_completionTime = cloudlet_new.getCloudletLength() / capacity + minCompletionTime_vm.getValue();
		                   
						    if(!displayStartupTimeMap.containsKey(cloudlet.getCloudletVideoId()) || cloudlet.getCloudletId() == 0){	
						         displayStartupTimeMap.put(cloudlet.getCloudletVideoId(), estimated_completionTime + CloudSim.clock());
								// cloudlet.setCloudletDeadline(minToCompleteTime);
							
								 for(VideoSegment vs:cloudletNewList){
									 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }
								 for(VideoSegment vs:cloudletList){
									 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }	 
							}
					       
					       
					       
					}
	        //If new arrival queue is empty, checek batch queue
	        }else{	
	        	if(getCloudletList().size() > 0){
	        
			       cloudlet_batch =(VideoSegment) getCloudletList().get(0);
			       switch_flag = 0;
			       
			       /**
			        * reset cloudlet deadline
			        */
			       // calculate the expected time for cloudlet completion
					if (cloudlet_batch.getVmId() == -1) {
						//vm = getVmsCreatedList().get(vmIndex);
						vm = (TranscodingVm) getVmList().get(vmIndex);

					} else { // submit to the specific vm
						vm = (TranscodingVm) VmList.getById(getVmList(), cloudlet_batch.getVmId());
						if (vm == null) { 
							// vm was not created
							/*Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
									+ cloudlet.getCloudletId() + ": bount VM not available");*/
							Log.printLine(getName() + ": Postponing execution of cloudlet "
									+ cloudlet_batch.getCloudletId() + ": bount VM not available");
							return;
						}
					}	
					
			    	//check vm type
                    String vmType = vm.getVmType();
			    	

			    	long cloudletLengthVm = cloudlet_batch.getCloudletLengthMap().get(vmType);
			    	cloudlet_batch.setCloudletLength(cloudletLengthVm);
			       
			      /* *//**
			    	 * Calculate random smaple from normal distribution
			    	 *//*
			    	Random r = new Random();
			    	double val = r.nextGaussian()*cloudlet.getCloudletStd() + cloudlet.getAvgCloudletLength();
			    	long sampleLength = (long) Math.round(val);
			    	
			    	long cloudletLengthVm = cloudlet_new.getCloudletLengthMap().get(vmType);
			    	cloudlet_new.setCloudletLength(cloudletLengthVm);*/
			    	
			    	cloudlet = cloudlet_batch;
			    	
			       
			    //if both new arrival and batch queue are empty, return and stop sending cloudlets
	        	}else {
	        		return;
	        	}
	        }
			
		}else{
			
			/*
			 * without new arrival queue
			 */

			
			//get the first cloudlet in the batch queue
			if(getCloudletList().size() > 0){
				
				/**
				 * 1. find the group of cloudlets that is the first GOP of each video
				 * 
				 */
				//for debug
				
				int newVideoNum = 0;
				
				VideoSegment vseg;
				for(Cloudlet cl:getCloudletList()){
					vseg = (VideoSegment)cl;
					
					if(vseg.getOrderNum() == 0 && !highestPriorityCloudletList.contains(vseg)){
						
						highestPriorityCloudletList.add(vseg);	
						
						for(VideoSegment vs:highestPriorityCloudletList){		    	
					    	if(vs.getUtilityNum() == 2){
					    		newVideoNum ++;
				    		}		
					    }
						//System.out.println("\n******Video Stream " + vseg.getCloudletVideoId() + " GOP " + vseg.getCloudletId() + " just joined highprioritylist...");
						//System.out.println("\n******The size of highprioritylist now is " + highestPriorityCloudletList.size() + "\n");

					}
					
				}
				
				Map<VideoSegment, Integer> minCompletionTime_cloudletToVmMap = new HashMap<VideoSegment, Integer>();
				Map<VideoSegment, Double> minCompletionTime_cloudletToTimeMap = new HashMap<VideoSegment, Double>();
				Map<Integer, Double> minCompletionTime_vmMap = new HashMap<Integer, Double>();
				
				//Map.Entry<String, Double> minMinCompletionTime = null;
				int minCompletionVmIndex = 0;
				double minCompletionTime = Double.MAX_VALUE;
				VideoSegment minCompletionTimeCloudlet = null;
				List<VideoSegment> newVideoList = new ArrayList<VideoSegment>();
				
				double cloudletEstimatedCompletionTime = 0.0;
				
				//if(schedulingmethod.equals("MM")){
					/**
					 * 2. implement original scheduling Method: MM, MSD, MMU
					 * 
					 */
					
					//a. Find the first available M
			  	
		    if(schedulingmethod.equals("MM") || schedulingmethod.equals("MSD") || schedulingmethod.equals("MMU")){
	
				/*for(Cloudlet cl:getCloudletList()){
					VideoSegment vsg = (VideoSegment)cl;*/
					//Calculate each cloudlet's completion time on every VM
					
			    for(VideoSegment vsg:highestPriorityCloudletList){	
					
					    Map<Integer, Double> tempTotalCompletionTime_vmMap = new HashMap<Integer, Double>(totalCompletionTime_vmMap);
					
						for(Integer vmNum:tempTotalCompletionTime_vmMap.keySet()){			
							
							if (vsg.getVmId() == -1) {
								vm = (TranscodingVm) getVmList().get(vmNum);
								boolean flag = vm.getDeallocationFlag();
								double rt = vm.getRemainingTime();
								//Check if this vm is about to be deallocated or renting time is over
								if(vm.getDeallocationFlag() && vm.getRemainingTime() <= 0){
									
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
									/*VideoSchedulerSpaceShared vmcsch = (VideoSchedulerSpaceShared) vm.getCloudletScheduler();														    	
							    	if(vmcsch.getCloudletExecList().size() == 0 && vmcsch.getCloudletWaitingList().size() == 0 && vmcsch.getCloudletPausedList().size() == 0){
								    	//System.out.println(CloudSim.clock() + "\n********************VM_" + vmToSend.getId() + "'s renting time is over and to be destroyed***********************" );
							    		sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
						                vmDestroyedList.add(vm);
						                
						                //set Vm's finish time.
						                vm.setVmFinishTime(CloudSim.clock());
						    	        
						                //Calculate vm cost based on the time it last.
						                setVmCost(vm);
							      }*/
							    	
							      totalCompletionTime_vmMap.remove(vmNum);
							    	
							      continue;
								}else if(!vm.getDeallocationFlag() && vm.getRemainingTime() <= 0 ){
									if(vm.getPeriodicUtilizationRate() > 0.2){
								    	 vm.setRentingTime(rentingTime);
										 System.out.println("\n****** VM" +vm.getId() + " has bee recharged from submit cloudlet*****");
									}else{
										System.out.println("\n****** VM" +vm.getId() + " renting time expired and utilization is too low, therefore set deallocation flag and remove it*****");
										vm.setDeallocationFlag(true);	
										//Before destroying this vm, make sure all the cloudlets in this vm are finished.
										VideoSchedulerSpaceShared scheduler = (VideoSchedulerSpaceShared)vm.getCloudletScheduler();
								    	if(scheduler.getCloudletExecList().size() == 0 && scheduler.getCloudletWaitingList().size() == 0 && scheduler.getCloudletPausedList().size() == 0) {
								    		
								    		System.out.println(CloudSim.clock() + "\n********************Cloudles in VM_" + vm.getId() + " have finished***********************" );
								    		sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
							                vmDestroyedList.add(vm);
							                
							                //set Vm's finish time.
							                vm.setVmFinishTime(CloudSim.clock());
							    	      
							                //Calculate vm cost based on the time it last.
							                setVmCost(vm);
								    	}
								    	totalCompletionTime_vmMap.remove(vmNum);
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
							VideoSchedulerSpaceShared vcschTemp = (VideoSchedulerSpaceShared) vm.getCloudletScheduler();
							double capacity = 0.0;
							int cpus = 0;
							for (Double mips : vcschTemp.getCurrentMipsShare()) {
								capacity += vm.getMips();
								if (mips > 0) {
									cpus++;
								}
							}
					
							int currentCpus = cpus;
							capacity /= cpus;
							
					    	//check vm type
	                        String vmType = vm.getVmType(); 
					    	
					    	Random r = new Random();
					    	double val = r.nextGaussian()*vsg.getCloudletStd() + vsg.getAvgCloudletLength();
					    	long sampleLength = (long) Math.round(val);
					    	
					    	long cloudletLengthVm = vsg.getCloudletLengthMap().get(vmType);
					    	vsg.setCloudletLength(cloudletLengthVm);
							
					    	cloudletEstimatedCompletionTime = vsg.getCloudletLength() / capacity + totalCompletionTime_vmMap.get(vmNum) + CloudSim.clock();
					    	
					    	minCompletionTime_vmMap.put(vmNum, cloudletEstimatedCompletionTime);
					    	
					    	if(!displayStartupTimeMap.containsKey(vsg.getCloudletVideoId()) || vsg.getCloudletId() == 0){	
						         displayStartupTimeMap.put(vsg.getCloudletVideoId(), cloudletEstimatedCompletionTime);
								// cloudlet.setCloudletDeadline(minToCompleteTime);
							
								 for(VideoSegment vs:cloudletList){
									 if(vs.getCloudletVideoId() == vsg.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }	 
							}
					     }
					  
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
					
						/*for(Cloudlet cl:getCloudletList()){
							VideoSegment vsg = (VideoSegment)cl;	*/
						for(VideoSegment vsg:highestPriorityCloudletList){
								 													
							 if(minCompletionTime_cloudletToTimeMap.get(vsg) < minCompletionTime) {		
								 
								 minCompletionTime = minCompletionTime_cloudletToTimeMap.get(vsg);
								 minCompletionTimeCloudlet = vsg;
								 minCompletionVmIndex = minCompletionTime_cloudletToVmMap.get(vsg);	
								 
							 }
								 					
						}
					}else if(schedulingmethod.equals("MSD")){
					
		                double minDeadline = Double.MAX_VALUE;
						
						//Find the minimum completion time cloudlet and VM# 
						/*for(Cloudlet cl:getCloudletList()){	
							VideoSegment vsg = (VideoSegment)cl;*/
		                for(VideoSegment vsg:highestPriorityCloudletList){
							
							 //put all the new videos in a list
							 if(vsg.getCloudletId() == 0){
								 newVideoList.add(vsg);				
							 }
							 
							 if(vsg.getCloudletDeadline() < minDeadline) {		
								 
								 minDeadline = vsg.getCloudletDeadline();
								 minCompletionTimeCloudlet = vsg;
								 minCompletionVmIndex = minCompletionTime_cloudletToVmMap.get(vsg);	
								 
							 }
								 													
							 
								 					
						}
					}else if(schedulingmethod.equals("MMU")){
						 /*
						  * b.After we find the first M for each cloudlet, we begin to find the minimum utility for MMU
						  */
						double minUtility = Double.MAX_VALUE;
						
						//Find the minimum completion time cloudlet and VM# 
						/*for(Cloudlet cl:getCloudletList()){	
							
							VideoSegment vsg = (VideoSegment)cl;*/
						for(VideoSegment vsg:highestPriorityCloudletList){
							 //put all the new videos in a list
							 if(vsg.getCloudletId() == 0){
								 newVideoList.add(vsg);
							 }
							 
							 double cloudletUtility = vsg.getCloudletDeadline() - minCompletionTime_cloudletToTimeMap.get(vsg);
							 if(cloudletUtility < minUtility) {		
								 
								 minUtility = cloudletUtility;
								 minCompletionTimeCloudlet = vsg;
								 minCompletionVmIndex = minCompletionTime_cloudletToVmMap.get(vsg);	
								 
							 }
									 					
						 }
				     }
				}else{
				
					/**
					 * 2. implement proposed scheduling Method: MM_Utily, MSD_Utility, MMU_Utilty
					 * 
					 */
					
					//a. Find the first available M
					
					for(VideoSegment vsg:highestPriorityCloudletList){
						
						//Calculate each cloudlet's completion time on every VM
						
						 Map<Integer, Double> tempTotalCompletionTime_vmMap = new HashMap<Integer, Double>(totalCompletionTime_vmMap);
						 		
						 for(Integer vmNum:tempTotalCompletionTime_vmMap.keySet()){			
							
					         if (vsg.getVmId() == -1) {
									vm = (TranscodingVm) getVmList().get(vmNum);
									boolean flag = vm.getDeallocationFlag();
									double rt = vm.getRemainingTime();
									//Check if this vm is about to be deallocated or renting time is over
									if(vm.getDeallocationFlag() && vm.getRemainingTime() <= 0){
										//Sometimes Vm has been destroyed by Coordinator, totalCompletionTime_vmMap hasn't updated yet, so need to check if vm has been destoryed or not
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
								    	
								        continue;
									}else if(!vm.getDeallocationFlag() && vm.getRemainingTime() <= 0 ){
										//System.out.println("\n***VM" + vm.getId() + " utilization is " + vm.getPeriodicUtilizationRate() + " and dealocation flag is " + vm.getDeallocationFlag() + " from submit cloudlet*****");

										if(vm.getPeriodicUtilizationRate() > 0.2){
									    	 vm.setRentingTime(rentingTime);
											 System.out.println("\n****** VM" +vm.getId() + " has bee recharged from submit cloudlet*****");
										}else{
											System.out.println("\n****** VM" +vm.getId() + " renting time expired and utilization is too low, therefore set deallocation flag and remove it from submit cloudlet*****");
											vm.setDeallocationFlag(true);	
											//Before destroying this vm, make sure all the cloudlets in this vm are finished.
											VideoSchedulerSpaceShared scheduler = (VideoSchedulerSpaceShared)vm.getCloudletScheduler();
									    	if(scheduler.getCloudletExecList().size() == 0 && scheduler.getCloudletWaitingList().size() == 0 && scheduler.getCloudletPausedList().size() == 0) {
									    		
									    		System.out.println(CloudSim.clock() + "\n********************Cloudles in VM_" + vm.getId() + " have finished from submit cloudlet***********************" );
									    		sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
								                vmDestroyedList.add(vm);
								                
								                //set Vm's finish time.
								                vm.setVmFinishTime(CloudSim.clock());
								    	      
								                //Calculate vm cost based on the time it last.
								                setVmCost(vm);
									    	}
									    	totalCompletionTime_vmMap.remove(vmNum);
									    	continue;
									    	
										}
								   	}
								} else { // submit to the specific vm
									vm = (TranscodingVm) VmList.getById(getVmList(), vsg.getVmId());
									if (vm == null) { 
										// vm was not created
										Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
												+ vsg.getCloudletId() + ": bount VM not available");
										Log.printLine(getName() + ": Postponing execution of cloudlet "
												+ vsg.getCloudletId() + ": bount VM not available");
										return;
									}
								}	
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
						
								int currentCpus = cpus;
								capacity /= cpus;
								
						    	//check vm type
		                        String vmType = vm.getVmType(); 
						    	
						    	Random r = new Random();
						    	double val = r.nextGaussian()*vsg.getCloudletStd() + vsg.getAvgCloudletLength();
						    	long sampleLength = (long) Math.round(val);
						    	
						    	
						    	
						    	long cloudletLengthVm = vsg.getCloudletLengthMap().get(vmType);
						    	vsg.setCloudletLength(cloudletLengthVm);
								
						    	cloudletEstimatedCompletionTime = vsg.getCloudletLength() / capacity + totalCompletionTime_vmMap.get(vmNum) + CloudSim.clock();
						    	
						    	minCompletionTime_vmMap.put(vmNum, cloudletEstimatedCompletionTime);
						    	
						    	
						   }
						  
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
						   
						   if(!displayStartupTimeMap.containsKey(vsg.getCloudletVideoId()) || vsg.getCloudletId() == 0){	
						         displayStartupTimeMap.put(vsg.getCloudletVideoId(), cloudletEstimatedCompletionTime);
								// cloudlet.setCloudletDeadline(minToCompleteTime);
							
								 for(VideoSegment vs:cloudletList){
									 if(vs.getCloudletVideoId() == vsg.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }	 
						}
				  }	
					 
				 if(schedulingmethod.equals("MMUT")){
					 //b.After we find the first M for each cloudlet, we begin to find the second M for MM		
					
					//Find the minimum completion time cloudlet and VM# 
					for(VideoSegment vsg:highestPriorityCloudletList){	
						 
						 //put all the new videos in a list
						 if(vsg.getCloudletId() == 0){
							 
							 newVideoList.add(vsg);
						 
						 }
							 													
						 if(minCompletionTime_cloudletToTimeMap.get(vsg) < minCompletionTime) {		
							 
							 minCompletionTime = minCompletionTime_cloudletToTimeMap.get(vsg);
							 minCompletionTimeCloudlet = vsg;
							 minCompletionVmIndex = minCompletionTime_cloudletToVmMap.get(vsg);	
							 
						 }
					}
							 					
				  }else if(schedulingmethod.equals("MSDUT")){
			
			
					 /*
					  * b.After we find the first M for each cloudlet, we begin to find the minimum deadline for MSD
					  */
					double minDeadline = Double.MAX_VALUE;
					
					//Find the minimum completion time cloudlet and VM# 
					for(VideoSegment vsg:highestPriorityCloudletList){	
						
						 //put all the new videos in a list
						 if(vsg.getCloudletId() == 0){
							 newVideoList.add(vsg);				
						 }
						 
						 if(vsg.getCloudletDeadline() < minDeadline) {		
							 
							 minDeadline = vsg.getCloudletDeadline();
							 minCompletionTimeCloudlet = vsg;
							 minCompletionVmIndex = minCompletionTime_cloudletToVmMap.get(vsg);	
							 
						 }
				
					}
					
				   /*TranscodingVm tvm = (TranscodingVm) getVmList().get(minCompletionVmIndex);
				   if(!tvm.getDeallocationFlag() && tvm.getRemainingTime() <= 0 ){
				    	 tvm.setRentingTime(rentingTime);
						 System.out.println("\n****** VM" +tvm.getId() + " has been recharged from submit cloudlet*****");
				   }*/
				 }else if(schedulingmethod.equals("MMUUT")){
			
				
					/*
					  * b.After we find the first M for each cloudlet, we begin to find the minimum utility for MMU
					  */
					double minUtility = Double.MAX_VALUE;
					
					//Find the minimum completion time cloudlet and VM# 
					for(VideoSegment vsg:highestPriorityCloudletList){	
						
						 //put all the new videos in a list
						 if(vsg.getCloudletId() == 0){
							 newVideoList.add(vsg);
						 }
						 
						 double cloudletUtility = vsg.getCloudletDeadline() - minCompletionTime_cloudletToTimeMap.get(vsg);
						 if(cloudletUtility < minUtility) {		
							 
							 minUtility = cloudletUtility;
							 minCompletionTimeCloudlet = vsg;
							 minCompletionVmIndex = minCompletionTime_cloudletToVmMap.get(vsg);	
							 
						 }
								 					
					 }
				 }
				 
				 
					
					
				/**
				 * Consider give higher priority to higher utility Cloudlet
				 */
			    Map<Integer, Double>tempTotalCompletionTime_vmMap = new HashMap<Integer, Double>(totalCompletionTime_vmMap);
                double highestUtility = minCompletionTimeCloudlet.getUtilityNum(); 	
                VideoSegment highestUtilityCloudlet = null;
                
                
				//Check if there are higher utility cloudlet will share VM with this cloudlet
                //vsg.getUtilityNum() > highestUtility
                
               
               
            	for(VideoSegment vsg:highestPriorityCloudletList){	
					if(!vsg.equals(minCompletionTimeCloudlet) && vsg.getUtilityNum() > highestUtility && minCompletionTime_cloudletToVmMap.get(vsg) == minCompletionTime_cloudletToVmMap.get(minCompletionTimeCloudlet)){								
						    highestUtility = vsg.getUtilityNum();
							highestUtilityCloudlet = vsg;
					}
				 }
                
               
				
				  //If they share the same VM, try map new video first if it won't incure minimum completion time
				  //cloudlet miss its deadline, otherwise don't map new video
				
				if(highestUtilityCloudlet != null){
				
	                if(!minCompletionTimeCloudlet.equals(highestUtilityCloudlet) && highestUtilityCloudlet.getUtilityNum() > 0){
	                
	               // if(!minCompletionTimeCloudlet.equals(highestUtilityCloudlet) && (highestUtilityCloudlet.getUtilityNum() -  minCompletionTimeCloudlet.getUtilityNum()) > 0.5){
	
	  				    	tempTotalCompletionTime_vmMap.put(minCompletionTime_cloudletToVmMap.get(highestUtilityCloudlet), minCompletionTime_cloudletToTimeMap.get(highestUtilityCloudlet)-CloudSim.clock());
	  				    	
	  				    	//it needs to run MM again
	
				    		Map<VideoSegment, Integer> tminCompletionTime_cloudletToVmMap = new HashMap<VideoSegment, Integer>();
	  						Map<VideoSegment, Double> tminCompletionTime_cloudletToTimeMap = new HashMap<VideoSegment, Double>();
	  						Map<Integer, Double> tminCompletionTime_vmMap = new HashMap<Integer, Double>();
	  						
	  						double tcloudletEstimatedCompletionTime = 0.0;
	  						
	  				    	for(VideoSegment tvsg:highestPriorityCloudletList){
	  							
		  						
	  							//Calculate each cloudlet's completion time on every VM
	  							if(!tvsg.equals(highestUtilityCloudlet)){
		  							for(Integer vmNum:tempTotalCompletionTime_vmMap.keySet()){			
		  								
		  								if (tvsg.getVmId() == -1) {
		  									vm = (TranscodingVm) getVmList().get(vmNum);
		  			
		  								} else { // submit to the specific vm
		  									vm = (TranscodingVm) VmList.getById(getVmList(), tvsg.getVmId());
		  									if (vm == null) { 
		  										
		  										Log.printLine(getName() + ": Postponing execution of cloudlet "
		  												+ tvsg.getCloudletId() + ": bount VM not available");
		  										return;
		  									}
		  								}	
		  								//Set Each VM CloudletWaitingList to 2, so if a VM's waitingList is beyound 2, current cloudlet 
		  								//won't be sent to specific vm.
		  								VideoSchedulerSpaceShared tvcschTemp = (VideoSchedulerSpaceShared) vm.getCloudletScheduler();
		  								double tcapacity = 0.0;
		  								int tcpus = 0;
		  								for (Double mips : tvcschTemp.getCurrentMipsShare()) {
		  									tcapacity += vm.getMips();
		  									if (mips > 0) {
		  										tcpus++;
		  									}
		  								}
		  						
		  								int tcurrentCpus = tcpus;
		  								tcapacity /= tcpus;
		  								
		  						    	//check vm type
		  		                        String tvmType = vm.getVmType(); 
		  						    	
		  						    	Random r = new Random();
		  						    	double val = r.nextGaussian()*tvsg.getCloudletStd() + tvsg.getAvgCloudletLength();
		  						    	long sampleLength = (long) Math.round(val);
		  						    	
		  						    	long tcloudletLengthVm = tvsg.getCloudletLengthMap().get(tvmType);
		  						    	tvsg.setCloudletLength(tcloudletLengthVm);
		  								
		  						    	tcloudletEstimatedCompletionTime = tvsg.getCloudletLength() / tcapacity + tempTotalCompletionTime_vmMap.get(vmNum) + CloudSim.clock();
		  						    	
		  						    	tminCompletionTime_vmMap.put(vmNum, tcloudletEstimatedCompletionTime);
		  						   }
		  						  
		  						   //After calculate each cloudlet's completion time on every VM, find and store the minimum completion time VM to that cloudlet	
		  						   minCompletionTime_vm = null;
		  						   for (Map.Entry<Integer, Double> entry : tminCompletionTime_vmMap.entrySet()) {
		  						      if (minCompletionTime_vm == null || minCompletionTime_vm.getValue() >= entry.getValue()) {
		  						           minCompletionTime_vm = entry;
		  						      }
		  						   }
		  						   if(minCompletionTime_vm == null){
		  							   vmIndex = 0;
		  						   }else {
		  					           vmIndex = minCompletionTime_vm.getKey();
		  						   }
		  						   
		  						   tminCompletionTime_cloudletToVmMap.put(tvsg, vmIndex);
		  						   tminCompletionTime_cloudletToTimeMap.put(tvsg, minCompletionTime_vm.getValue());
		  					   }
	  				        }	
		  						 
		  					 
		  					 //b.After we find the first M for each cloudlet, we begin to find the second M for MM
	 
			  					 //Map.Entry<String, Double> minMinCompletionTime = null;
		  					
		  					 //Find the last minimum cloudlet's new completion time cloudlet and VM# 
		  					// for(VideoSegment tvsg:highestPriorityCloudletList){	
	  				    	 /*if(CloudSim.clock() > 611016){
	  		                	System.out.println("test");
	  		                 
	  				    	 }*/
	  				    	 
	  				    	 double test1 = tminCompletionTime_cloudletToTimeMap.get(minCompletionTimeCloudlet);
	  				    	 double test2 = minCompletionTimeCloudlet.getCloudletDeadline();
	  				    	 
	  				    	
	  						 if(tminCompletionTime_cloudletToTimeMap.get(minCompletionTimeCloudlet) <= (minCompletionTimeCloudlet.getCloudletDeadline() - 500)){
	  							 //check if it will miss deadline or not
	  							//increase former minCompletionTimeCloudlet utility
	  							double upgradedUtilityNumber = minCompletionTimeCloudlet.getUtilityNum();
	  							minCompletionTimeCloudlet.setUtilityNum(upgradedUtilityNumber);
	  							
	  						    System.out.println("\n*************High utility GOP: " + highestUtilityCloudlet.getCloudletId() + " from Video #"+ highestUtilityCloudlet.getCloudletVideoId() +   " replaced the minComp GOP " + minCompletionTimeCloudlet.getCloudletId() + " from Video #"+minCompletionTimeCloudlet.getCloudletVideoId() + " at VM#" + minCompletionTime_cloudletToVmMap.get(highestUtilityCloudlet) + "\n");

	  							
	  							minCompletionTime = minCompletionTime_cloudletToTimeMap.get(highestUtilityCloudlet);
	  						    minCompletionTimeCloudlet = highestUtilityCloudlet;
	  						    minCompletionVmIndex = minCompletionTime_cloudletToVmMap.get(highestUtilityCloudlet);	
	  						    
	  						 
	  						 }
	
	  				   }
				   }
			 }

			 cloudlet = minCompletionTimeCloudlet;
					
			 if(!displayStartupTimeMap.containsKey(cloudlet.getCloudletVideoId()) || cloudlet.getCloudletId() == 0){	
				 
		         displayStartupTimeMap.put(cloudlet.getCloudletVideoId(), cloudletEstimatedCompletionTime);		         
			
				 for(VideoSegment vs:cloudletList){
					 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
					    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
					 }
				 }	 
			 }
		   
				
	      }else{
			return;
	      }
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
	
			
			Log.printLine(CloudSim.clock() + getName() + ": Sending Video ID: " + cloudlet.getCloudletVideoId() +" Cloudlet "
						+ cloudlet.getCloudletId() + " to VM #" + vm.getId());
			cloudlet.setVmId(vm.getId());
			
	        EventData data = new EventData(cloudlet, totalCompletionTime_vmMap);
	       //totalCompletionTime_vmMap_Min.put(vmIndex, estimated_completionTime);
	        
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, data);
			cloudletsSubmitted++;
					
			getCloudletSubmittedList().add(cloudlet);	
			
			highestPriorityCloudletList.remove(cloudlet);
			
			/*
			 * Recalculate the GOP order number of that video
			 */
			int sentVideoId = cloudlet.getCloudletVideoId();
			int orderNumber;
			for(Cloudlet clt:getCloudletList()){
				VideoSegment vsg = (VideoSegment)clt;
				
				if(vsg.getCloudletVideoId() == sentVideoId){
					
					orderNumber = vsg.getOrderNum();
					vsg.setOrderNum(orderNumber - 1);
				}	
			}
			
			
			//remove this cloudlet from cloudlet list
			if(startupqueue){
				if(switch_flag == 0){
					cloudlet.setSegmentExecutedVmType(vm.getVmType());
					getCloudletList().remove(cloudlet);
				}else {
					cloudlet.setSegmentExecutedVmType(vm.getVmType());
					getCloudletNewList().remove(cloudlet);
				}
			}else{
			
				cloudlet.setSegmentExecutedVmType(vm.getVmType());
				getCloudletList().remove(cloudlet);
			}

		}
      
	}
	
	public double getEstimatedCompletionTime(VideoSegment vsg, TranscodingVm vm, int vmNum){
		
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

		int currentCpus = cpus;
		capacity /= cpus;
		
    	//check vm type
        String vmType = vm.getVmType(); 
    	
    	Random r = new Random();
    	double val = r.nextGaussian()*vsg.getCloudletStd() + vsg.getAvgCloudletLength();
    	long sampleLength = (long) Math.round(val);
    	
    	long cloudletLengthVm = vsg.getCloudletLengthMap().get(vmType);
    	vsg.setCloudletLength(cloudletLengthVm);
		
    	cloudletEstimatedCompletionTime = vsg.getCloudletLength() / capacity + totalCompletionTime_vmMap.get(vmNum) + CloudSim.clock();
	    
    	if(!displayStartupTimeMap.containsKey(vsg.getCloudletVideoId()) || vsg.getCloudletId() == 0){	
	         displayStartupTimeMap.put(vsg.getCloudletVideoId(), cloudletEstimatedCompletionTime);
			// cloudlet.setCloudletDeadline(minToCompleteTime);
		
			 for(VideoSegment vs:cloudletList){
				 if(vs.getCloudletVideoId() == vsg.getCloudletVideoId()){
				    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
				 }
			 }	 
		}
    	
    	return cloudletEstimatedCompletionTime;
    	
	}
	
	protected void processCloudletReturn(SimEvent ev) {
		
		//Coordinator coordinator;
		VideoSegment cloudlet = (VideoSegment) ev.getData();
		getCloudletReceivedList().add(cloudlet);
		
		
	/*	Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId()
				+ " finished in VM" + cloudlet.getVmId());*/
		Log.printLine(CloudSim.clock() + getName() + " : Video Id" + cloudlet.getCloudletVideoId() + " Cloudlet " + cloudlet.getCloudletId()
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
			/*if(vm.getVmType().equals("t2.small")){
				System.out.println("test");
			}*/
			//double mips = vm.getMips();
			//calculate the difference between the real exec time and estimated exec time
			double estimatedExecTime = cloudlet.getCloudletLength() / vm.getMips();
			double execTime_dif = execTime - estimatedExecTime;

			
			totalCompletionTime_temp = totalCompletionTime_vmMap.get(vmId) - execTime + execTime_dif;
			totalCompletionTime_vmMap.put(vmId, totalCompletionTime_temp);		
		}
		
		
		//get this video's start up time
		if(cloudlet.getCloudletId() == 0){
			double videoStartupTime =0;
			videoStartupTime = cloudlet.getFinishTime() - cloudlet.getArrivalTime();
			videoStartupTimeMap.put(cloudlet.getCloudletVideoId(), videoStartupTime);
		}
		
		/*
		 * Remodify videos' deadline based on the real display time
		 */
		if(!displayStartupTimeRealMap.containsKey(cloudlet.getCloudletVideoId()) && cloudlet.getCloudletId() == 0){	
			// cloudlet.setCloudletDeadline(minToCompleteTime);
	         displayStartupTimeRealMap.put(cloudlet.getCloudletVideoId(), cloudlet.getFinishTime());
             cloudlet.setCloudletDeadline(cloudlet.getFinishTime());
	         

			 for(VideoSegment vs:cloudletNewList){
				 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
				    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeRealMap.get(vs.getCloudletVideoId()));	
				 }
			 }
			 for(VideoSegment vs:cloudletList){
				 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
				    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeRealMap.get(vs.getCloudletVideoId()));	
				 }
			 }		 
		}/*else{
			cloudlet.setCloudletDeadline(cloudlet.getCloudletDeadlineAfterPlay() + displayStartupTimeRealMap.get(cloudlet.getCloudletVideoId()));
		}*/
		
		/*if(CloudSim.clock() > 138399){
			System.out.println("test");
		}
		*/
        
		//Check if this vm is set to be destroyed
		if(vm.getDeallocationFlag() && vm.getRemainingTime() <= 0) {	
			
			//Before destroying this vm, make sure all the cloudlets in this vm are finished.
			VideoSchedulerSpaceShared scheduler = (VideoSchedulerSpaceShared)vm.getCloudletScheduler();
	    	if(scheduler.getCloudletExecList().size() == 0 && scheduler.getCloudletWaitingList().size() == 0 && scheduler.getCloudletPausedList().size() == 0) {
	    		
	    		System.out.println(CloudSim.clock() + "\n********************Cloudles in VM_" + vm.getId() + " have finished from cloudlet return***********************" );
	    		sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
                vmDestroyedList.add(vm);
                
                //set Vm's finish time.
                vm.setVmFinishTime(CloudSim.clock());
    	      
                //Calculate vm cost based on the time it last.
                setVmCost(vm);         
                
	    	}
	    	totalCompletionTime_vmMap.remove(vm.getId());
	    	
		}else if(!vm.getDeallocationFlag() && vm.getRemainingTime() <= 0 ){
			if(vm.getPeriodicUtilizationRate() > 0.2){
		    	 vm.setRentingTime(rentingTime);
				 System.out.println("\n****** VM" +vm.getId() + " has bee recharged from cloudlet return*****");
			}else{
				
				/*double highestUtilizationRate = 0.0;
				TranscodingVm highestUtilizationRateVm = null;
				int maxVmNum = getVmList().size();
				
				for(Vm tvm : getVmList()){
					TranscodingVm vmm = (TranscodingVm) tvm;					
					
					if(vmm.getPeriodicUtilizationRate() > highestUtilizationRate){
						highestUtilizationRate = vmm.getPeriodicUtilizationRate();
						highestUtilizationRateVm = vmm;
					}

				}
				if(highestUtilizationRate > 0.8){ 
				    System.out.println("\n****** VM" +vm.getId() + " renting time expired and utilization is too low, therefore transfom it to high utilization VM:" + highestUtilizationRateVm.getId() +" type" + highestUtilizationRateVm.getVmType() + " from cloudlet return*****");

                	//provisionVM(1,highestUtilizationRateVm.getVmType());
                	
                	List<TranscodingVm> vmNew = (List<TranscodingVm>) createVM(getId(), highestUtilizationRateVm.getVmType(), 1, maxVmNum, rentingTime);
                	
		            
		            //submit it to broker
					submitVmList(vmNew);
										
					//creat a event for datacenter to create a vm
			    	sendNow(getVmsToDatacentersMap().get(vm.getId()),CloudSimTags.VM_CREATE_ACK, vmNew.get(0));
			    }else{
			    
			        System.out.println("\n****** VM" +vm.getId() + " renting time expired and utilization is too low, therefore set deallocation flag and remove it from cloudlet return*****");
			    }*/
				
		        System.out.println("\n****** VM" +vm.getId() + " renting time expired and utilization is too low, therefore set deallocation flag and remove it from cloudlet return*****");
				
				vm.setDeallocationFlag(true);	

				//Before destroying this vm, make sure all the cloudlets in this vm are finished.
				VideoSchedulerSpaceShared scheduler = (VideoSchedulerSpaceShared)vm.getCloudletScheduler();
		    	if(scheduler.getCloudletExecList().size() == 0 && scheduler.getCloudletWaitingList().size() == 0 && scheduler.getCloudletPausedList().size() == 0) {
		    		
		    		System.out.println(CloudSim.clock() + "\n********************Cloudles in VM_" + vm.getId() + " have finished from cloudlet return***********************" );
		    		sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
	                vmDestroyedList.add(vm);
	                
	                //set Vm's finish time.
	                vm.setVmFinishTime(CloudSim.clock());
	    	      
	                //Calculate vm cost based on the time it last.
	                setVmCost(vm);
		    	}
		    	totalCompletionTime_vmMap.remove(vm.getId());
			}
	   	}
		
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
	   // List<? extends Cloudlet> cloudletNewArrivalQueue_temp = Collections.synchronizedList(new ArrayList<Cloudlet>());
	    List<Cloudlet> cloudletBatchQueue_temp = Collections.synchronizedList(new ArrayList<Cloudlet>());	
	    List<Cloudlet> cloudletNewQueue_temp = Collections.synchronizedList(new ArrayList<Cloudlet>());	
	   
	   // cloudletNewArrivalQueue_temp = cloudletNewArrivalQueue;
	    cloudletBatchQueue_temp.addAll(cloudletBatchQueue);
	    cloudletNewQueue_temp.addAll(cloudletNewArrivalQueue);
	    
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
		
		//getCloudletNewList().clear();
		

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
	
	//sorted by cloudlet length
		protected void FCFS() {
			List<VideoSegment> lstCloudlets = getCloudletList();
			List<VideoSegment> lstNewCloudlets = getCloudletNewList();
			
			for (int a = 0; a < lstCloudlets.size(); a++) {
	            for (int b = a + 1; b < lstCloudlets.size(); b++) {
	            	
	            	if(lstCloudlets.get(b).getArrivalTime() < lstCloudlets.get(a).getArrivalTime()){
                		VideoSegment temp = lstCloudlets.get(a);
	                    lstCloudlets.set(a, lstCloudlets.get(b));
	                    lstCloudlets.set(b, temp);
                	}
	                /*if(lstCloudlets.get(b).getCloudletVideoId() == lstCloudlets.get(a).getCloudletVideoId()){
	                	if (lstCloudlets.get(b).getCloudletDeadline() < lstCloudlets.get(a).getCloudletDeadline()) {
		                    VideoSegment temp = lstCloudlets.get(a);
		                    lstCloudlets.set(a, lstCloudlets.get(b));
		                    lstCloudlets.set(b, temp);
		                }
	                }else{
	                	
	                }*/
	            }
	        }
			setCloudletList(lstCloudlets);
			
			/*for(Cloudlet newcl:lstNewCloudlets){
	            System.out.println("Cloudlet id = " + newcl.getCloudletId() + " - Length = " + newcl.getCloudletLength());

			}

	        //Printing ordered list of cloudlets
	        for (Cloudlet cl : lstCloudlets) {
	            System.out.println("Cloudlet id = " + cl.getCloudletId() + " - Length = " + cl.getCloudletLength());
	        }*/
		}
		
		
		//sorted by cloudlet deadline
		protected void SortedbyDeadline() {
			List<VideoSegment> lstCloudlets = getCloudletList();
			List<VideoSegment> lstNewCloudlets = getCloudletNewList();
			
			for (int a = 0; a < lstCloudlets.size(); a++) {
	            for (int b = a + 1; b < lstCloudlets.size(); b++) {
	                if (lstCloudlets.get(b).getCloudletDeadline() < lstCloudlets.get(a).getCloudletDeadline()) {
	                    VideoSegment temp = lstCloudlets.get(a);
	                    lstCloudlets.set(a, lstCloudlets.get(b));
	                    lstCloudlets.set(b, temp);
	                }
	            }
	        }
			
			setCloudletList(lstCloudlets);

			/*for(VideoSegment newcl:lstNewCloudlets){
		           System.out.println("VIDEO ID: " + newcl.getCloudletVideoId() + " Cloudlet id = " + newcl.getCloudletId() + "   Deadline = " + newcl.getCloudletDeadline());

			}
			
			System.out.println("\n");*/
			
	        //Printing ordered list of cloudlets
	      /*  for (VideoSegment cl : lstCloudlets) {
	           System.out.println("VIDEO ID: " + cl.getCloudletVideoId() + " Cloudlet id = " + cl.getCloudletId() + "   Deadline = " + cl.getCloudletDeadline());
	        }*/
		}
		
	    //sorted by cloudlet's estimated shortest execution time (SJF)
		protected void SortedbySJF() {
			List<VideoSegment> lstCloudlets = getCloudletList();
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

	                                    VideoSegment temp1 = lstCloudlets.get(i);
	                                    lstCloudlets.set(i, lstCloudlets.get(j));
	                                    lstCloudlets.set(j, temp1);    
	                                    
	                                    double temp2 = executionTimeList.get(i);
	                                    executionTimeList.set(i, executionTimeList.get(j));
	                                    executionTimeList.set(j, temp2);

	                            }

	                            }
	                    }

	         setCloudletList(lstCloudlets);

	        /*System.out.println("After  ordering");
	         for(int i=0;i<reqTasks;i++) {
	        	 
		         System.out.println("VIDEO ID: " + lstCloudlets.get(i).getCloudletVideoId() +  " CLOUDLET ID" + " " +lstCloudlets.get(i).getCloudletId() +" EXE TIME   " +  executionTimeList.get(i));
	         }*/
			
		}
		
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
	   public List<VideoSegment> getHighestPriorityCloudletList() {
			return highestPriorityCloudletList;
		}
	   

	   /**
	    * Set priority queue	
	    * @return
	    */
		public void setHighestPriorityCloudletList(
				List<VideoSegment> highestPriorityCloudletList) {
			this.highestPriorityCloudletList = highestPriorityCloudletList;
		}
	  
		
		
		

}
