package cloudproject.transcoding;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.opencsv.CSVReader;
import java.text.*;



/**
 * An example showing how to create
 * scalable simulations.
 */
public class TranscodingMain {
	
	/** The vmlist. */
	private static List<Vm> vmlist;
	
	
	private static int vmIdShift = 1;

	
	/**
	 * Create datatcenter
	 */
	
	private static TranscodingDatacenter datacenter;
	
	/**
	 * Calculate the total cost
	 */
	public static DatacenterCharacteristics characteristics;
	public static double memCost = 0;
	public static double storageCost = 0;
	public static double bwCost = 0;
	public static double vmCost = 0;
	public static double totalCost = 0;
	public static double totalTranscodingTime = 0;
	
	/*
	 * URL file
	 */
	public static String propertiesFileURL = null;
	public static String inputdataFolderURL = null;
	public static String outputdataFileURL = null;
	public static String gopDelayOutputURL = null;
	public static String sortalgorithm = null;
	public static String schedulingmethod = null;
	public static boolean startupqueue = true;
	public static int jobNum = 0;
	public static int waitinglist_max = 0;
	public static int vmNum = 0;
	public static String clusterType = null;
	public static String vmType = null;
	public static int frequency = 0;
	public static String estimatedGopLength = null;
	public static int seedShift = 0;
	public static double upthredshold = 0.0;
	public static double lowthredshold = 0.0;
    public static long rentingTime = 0;
    public static double testPeriod = 0.0;
    public static boolean stqprediction = true;
	public static boolean dropflag = true;
	

	
			
	//create period event
	private final static int PERIODIC_EVENT = 127;
	
	
	//All the instance share cloudletNewArrivalQueue and cloudletBatchqueue, both of them are synchronized list
	//private static List<Binary> cloudletNewArrivalQueue = Collections.synchronizedList(new ArrayList<Binary>());
	//private static List<Binary> cloudletBatchQueue = Collections.synchronizedList(new ArrayList<Binary>());

	private static List<Binary> cloudletNewArrivalQueue = Collections.synchronizedList(new ArrayList<Binary>());
	private static List<Binary> cloudletBatchQueue = Collections.synchronizedList(new ArrayList<Binary>());

	public static Properties prop = new Properties();
	
	public TranscodingMain(){
		
	}
	
//////////////////////////STATIC METHODS ///////////////////////
	
	/**
	* Creates main() to run this example
	*/
	public static void main(String[] args) {
		
	printOutputFile pof = new printOutputFile();  
  	pof.printOutToFile("cloudTranscoding_console"); 	
		
	Log.printLine("Starting Video Transcoding Simulation...");
	
    Log.printLine("Seting up configuration property file...\n");	
    
   String[] arg = {"-property", "/home/yamini/Documents/resources/properties/config.properties", 
            //"-input", "/home/yamini/Documents/resources/inputdata_het",
		    "-input", "/home/yamini/Documents/resources/test.csv",
            "-output", "/home/yamini/Documents/resources/outputdata/test.txt", 
            "-gopdelayoutput", "/home/yamini/Documents/resources/outputdata/gop.txt",
            //"-sortalgorithm", "SJF",
            "-schedulingmethod", "MM",
           // "-startupqueue",
            //"-dropflag",
            "-stqprediction",
            "-videonum", "10",
            "-vmqueue", "1",
            "-vmNum", "2",  
            "-clusterType", "heterogeneous",
            "-vmType", "g2.2xlarge",
            "-vmfrequency", "10000",
            "-goplength", "AVERAGE",
            "-upthreshold", "0.05", 
            "-lowthreshold", "0.01",
            "-testPeriod", "1200000",
            "-rentingTime", "20000",
            "-seedshift", "3"};
    
	ParseCmdLine pcl = new ParseCmdLine();
	CmdLineParser parser = new CmdLineParser(pcl);
	try {
        parser.parseArgument(arg);
        //pcl.run(test);
        propertiesFileURL = pcl.getPropertiesFileURL();
		System.out.println("**Property file url: " + propertiesFileURL);
		
		inputdataFolderURL = pcl.getInputdataFolderURL();
	  	System.out.println("**Input folder url: " + inputdataFolderURL);
       
	  	outputdataFileURL = pcl.getOutputdataFileURL();
	  	System.out.println("**Output file url: " + outputdataFileURL);
	  	
	  	gopDelayOutputURL = pcl.getGopdelayoutput();
	  	System.out.println("**GOP delay output file url: " + outputdataFileURL);
	  	
	  	

	  	
	  	sortalgorithm = pcl.getSortAlgorithm();	  	
	  	startupqueue = pcl.getStarupQueue();	  	
	  	jobNum = pcl.getVideoNum();	  	
	  	waitinglist_max = pcl.getVmQueueSize();	  	
	  	vmNum = pcl.getVmNum();
	  	clusterType = pcl.getClusterType();
	  	vmType = pcl.getVmType();
	  	frequency = pcl.getVmFrequency();
	  	seedShift = pcl.getSeedShift();
	  	estimatedGopLength = pcl.getEstimatedGopLength();
	  	upthredshold = pcl.getUpThredshold();
	  	lowthredshold = pcl.getLowThreshold();
	  	rentingTime = pcl.getRentingTime();
	  	testPeriod = pcl.getTestPeriod();
	  	stqprediction = pcl.getStqPrediction();
		dropflag = pcl.getDropFlag();
		schedulingmethod = pcl.getSchedulingMethod();

	  	
	  	
	  	
    } catch (CmdLineException e) {
        // handling of wrong arguments
        System.err.println(e.getMessage());
        parser.printUsage(System.err);
    }
  	
	
    
    //set up configuration 
	
	OutputStream output = null;


	try {


		output = new FileOutputStream(propertiesFileURL);
		/**
		 * Configuration properties for datacenter 
		 */
		prop.setProperty("datacenterMips", "100000");
		prop.setProperty("datacenterHostId", "0");
		prop.setProperty("datacenterRam", "163840"); //host memory 16GB
		prop.setProperty("datacenterStorage", "10485760"); //1TB
		prop.setProperty("datacenterBw", "163840"); //16GB
		
		/**
		 * Configuration properties for VM
		 */
		
		prop.setProperty("vmSize", "30720"); //30GB
		prop.setProperty("vmRam", "1024"); //vm memory(MB)
		prop.setProperty("vmMips", "8000"); 
		prop.setProperty("vmBw", "1024"); //1GB
		prop.setProperty("vmPesNumber", "1"); //number of cups
		prop.setProperty("vmName", "Xeon"); 	
		
		if(vmNum == 0){
			prop.setProperty("vmNum", "1");
			System.out.println("**The vm policy is: dynamic");
		}else{
			prop.setProperty("vmNum", String.valueOf(vmNum));
			System.out.println("**The number of Vm is: " + vmNum);
		}
		
		if(clusterType == null)
		    prop.setProperty("clusterType", "homogeneous");
		else
			prop.setProperty("clusterType", clusterType);
		
		if(vmType == null)
		    prop.setProperty("vmType", "g2.2xlarge");
		else
			prop.setProperty("vmType", vmType);
		
		
		//Configure VM renting time
		if(rentingTime == 0){
		    prop.setProperty("rentingTime", "60000");
			System.out.println("**VM renting time is: 60000ms" );

		}else{
			prop.setProperty("rentingTime", String.valueOf(rentingTime));
			System.out.println("**VM renting time is: " + rentingTime +"ms");
		}
		
		
		/**
		 * Configuration properties for cost
		 */
		
		prop.setProperty("vmCostPerSec", "0.0000036");
		prop.setProperty("storageCostPerGb", "0.03"); 
		
		/**
		 * Configuration of GOP
		 */
		if(estimatedGopLength == null)
		    prop.setProperty("estimatedGopLength", "WORST");
		else
			prop.setProperty("estimatedGopLength", estimatedGopLength);
		
		/**
		 * Configuration properties for broker
		 */
		if(sortalgorithm == null){
		   prop.setProperty("sortalgorithm", "SDF");
		   System.out.println("**The sorting algorithm is: SDF...");
		}else{
		   prop.setProperty("sortalgorithm", sortalgorithm);
		   System.out.println("**The sorting algorithm is: " + sortalgorithm + "...");
		}
		
		if(schedulingmethod == null){
			   prop.setProperty("schedulingmethod", "MMUT");
			   System.out.println("**The scheduling method is: MMUT...");
			}else{
			   prop.setProperty("schedulingmethod", schedulingmethod);
			   System.out.println("**The scheduling method is: " + schedulingmethod + "...");
			}
		
		if(startupqueue == true){
		    prop.setProperty("startupqueue", "true");
		    System.out.println("**The process has startup queue....");
		}else{
			prop.setProperty("startupqueue", String.valueOf(startupqueue));
			System.out.println("**The process doesn't have startup queue...");
		}
		
		if(stqprediction == true){
			prop.setProperty("stqprediction", "true");
		    System.out.println("**The process has startup queue prediction....");
		}else{
			prop.setProperty("stqprediction", "false");
			System.out.println("**The process doesn't have startup queue prediction...");
		}
		
		if (dropflag == true) {
			prop.setProperty("dropflag", "true");
			System.out
					.println("**The process turned on drop flag....");
		} else {
			prop.setProperty("dropflag", "false");
			System.out
					.println("**The process turn off drop flag...");
		}
		
			prop.setProperty("seedShift", String.valueOf(seedShift));
		
		
		/**
		 * configuration properties in Cooridnator
		 */
		//video job number, value i means i videos
		
		File folder = new File(inputdataFolderURL);
		/*
        File[] listOfFiles = folder.listFiles();
        int jobCount = 0;
        
        for (int i = 0; i < listOfFiles.length; i++) {
        	File inputfile = listOfFiles[i];
        	if(inputfile.isFile() && inputfile.getName().toLowerCase().endsWith(".txt")){
        		jobCount++;
        	}
        	
        }
      
		//Calculating number of jobs, jobNum overrides number of files in the folder
        if(jobNum != 0){
		   prop.setProperty("periodEventNum", String.valueOf(jobNum));
		   System.out.println("**There are " + jobNum + " videos...");
        }else{
 		   prop.setProperty("periodEventNum", String.valueOf(jobCount));
		   System.out.println("**There are " + jobCount + " videos...");


        }
        
        */
		
		prop.setProperty("periodEventNum", "2");
		System.out.println("**There are  2 videos...");
		//FIXME!
		
		//check vm provision frequency for renting/destroying vms
        if(frequency ==0 ){
		   prop.setProperty("periodicDelay", "1000");	
		   System.out.println("**Vm checking frequency is: 1000ms");

        }else{
           prop.setProperty("periodicDelay", String.valueOf(frequency));
		   System.out.println("**Vm checking frequency is: " + frequency);

        }
        
        //configure test time period
        //Not sure what this is doing!!
        
        if(testPeriod == 0.0){
        	prop.setProperty("testPeriod", "1200000");
        	System.out.println("**Test Period is: 1200000");
        }else{
        	prop.setProperty("testPeriod", String.valueOf(testPeriod));
        	System.out.println("**Test Period is: " + testPeriod + "ms");
        }
        
        
        
		//The maximum number of vm a user can create
		prop.setProperty("MAX_VM_NUM", "100");
		
		/**
		 * configuration properties in broker and datacenter's VM local queue size
		 */
		
		//Local queue size
		if(waitinglist_max == 0){
		    prop.setProperty("waitinglist_max", "2");
			System.out.println("**Vm local waiting queue size is: 2");
		}else{
			prop.setProperty("waitinglist_max", String.valueOf(waitinglist_max));
			System.out.println("**Vm local waiting queue size is: "+ waitinglist_max);

		}

		/**
		 * configuration properties for transcoding provisioning 
		 * Not sure if this is useful for me!!
		 */
		if(upthredshold == 0){
		    prop.setProperty("DEADLINE_MISS_RATE_UPTH", "0.1");
		    System.out.println("**deadline miss rate upthredshold is: 0.1");
		}else{
			prop.setProperty("DEADLINE_MISS_RATE_UPTH", String.valueOf(upthredshold));
		    System.out.println("**deadline miss rate upthredshold is: " + upthredshold);

		}
		
		if(lowthredshold == 0){
		    prop.setProperty("DEADLINE_MISS_RATE_LOWTH", "0.05");
		    System.out.println("**deadline miss rate lowthredshold is: 0.05");
		    
		}else{
			prop.setProperty("DEADLINE_MISS_RATE_LOWTH", String.valueOf(lowthredshold));
		    System.out.println("**deadline miss rate lowthredshold is: " + lowthredshold);

		}
		prop.setProperty("ARRIVAL_RATE_TH", "0.5");
		
		System.out.println('\n');

		// save properties to project root folder
		prop.store(output, null);
		
		

	} catch (IOException io) {
		io.printStackTrace();
	} finally {
		if (output != null) {
			try {
				output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
	
	try {
		// First step: Initialize the CloudSim package. It should be called before creating any entities.
		int num_user = 1;   // number of grid users
		Calendar calendar = Calendar.getInstance();
		boolean trace_flag = false;  // mean trace events
		
		// Initialize the CloudSim library
		CloudSim.init(num_user, calendar, trace_flag);
		
		// Second step: Create Datacenters
		//Datacenters are the resource providers in CloudSim. We need at list one of them to run a CloudSim simulation
		@SuppressWarnings("unused")
		TranscodingDatacenter datacenter0 = createDatacenter("Datacenter_0");
		
		//choose datacenter0 as the datacenter to create new vm, which can improve later
		datacenter = datacenter0;
		
		@SuppressWarnings("unused")
	//	TranscodingDatacenter datacenter1 = createDatacenter("Datacenter_1");
		
		//Third step: Create Broker and VideoStreams
		TranscodingMain ct = new TranscodingMain();
        TranscodingMain.Coordinator coordinator = ct.new Coordinator("Coordinator");	
		//TranscodingMain.Coordinator coordinator = new Coordinator("Coordinator");	
		
		
       /* //Fourth Step: Create a initial VM list
		vmlist = createVM(coordinator.getBroker().getId(), 2); //creating vms
		coordinator.getBroker().submitVmList(vmlist);*/

		double startTranscodingTime = System.currentTimeMillis();
		// Fifth step: Starts the simulation
		
		System.out.println("**Starting Simulation...");
		
		CloudSim.startSimulation();
		
		totalTranscodingTime = (System.currentTimeMillis() - startTranscodingTime)/1000;
		
		// Final step: Print results when simulation is over
		List<Binary> newList = coordinator.getBroker().getCloudletSubmittedList();
				
		Map<Integer, Double> videoStartupTimeMap = coordinator.getBroker().getVideoStartupTimeMap();
		
		vmCost = coordinator.getBroker().getVmCost();
		storageCost = getStorageCost(newList);
		
		calculateTotalCost(storageCost, vmCost);	
		
		
		//Think of better functions here
		//printVideoStatistic(videoStartupTimeMap, newList);				
		//printCloudletList(newList);
		
		
		CloudSim.stopSimulation();
		 
		
		
		Log.printLine("Video Transcoding Simulation Finished!");
		}
	catch (Exception e)
		{
		e.printStackTrace();
		Log.printLine("The simulation has been terminated due to an unexpected error");
		}
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
		
		

		//create VMs
		TranscodingVm[] vm = new TranscodingVm[vms];

		for(int i=0;i<vms;i++){

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
    
	private static double getBwCost(List<Binary> list){
		for(Binary cl:list){
			bwCost += cl.getProcessingCost();
		}
		return bwCost;
	}
	
	private static double getStorageCost(List<Binary> list){
		double byteConvertToGb = 1024*1024*1024;
		double storageSize =0;
		for(Binary cl:list){
			storageSize += cl.getCloudletFileSize();
			storageSize += cl.getCloudletOutputSize();

		}
		System.out.println("The storage size is: " + new DecimalFormat("#0.00").format(storageSize));
		storageCost = storageSize/byteConvertToGb*characteristics.getCostPerStorage();
		
		return storageCost;
	}
	
	private static void calculateTotalCost(double storageCost, double vmCost){
		
		//totalCost = storageCost + vmCost;
		totalCost = vmCost;
		System.out.println("\n");
		System.out.println("*******The storage cost is: " + new DecimalFormat("#0.0000").format(storageCost));
		System.out.println("*******The time cost is: " + new DecimalFormat("#0.0000").format(vmCost));
		System.out.println("*******The total cost is: " + new DecimalFormat("#0.0000").format(totalCost));
		
		//return totalCost;
	}
    
	
    
	
	
	
	private static TranscodingDatacenter createDatacenter(String name){

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store one or more
		//    Machines
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
		//    create a list to store these PEs before creating
		//    a Machine.
		List<Pe> peList1 = new ArrayList<Pe>();

		//int mips = 8000;
		String mipsStr = prop.getProperty("datacenterMips", "8000");
        int mips = Integer.valueOf(mipsStr);

		// 3. Create PEs and add these into the list.
		//for a quad-core machine, a list of 4 PEs is required:
		peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
		peList1.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(4, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
		peList1.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(7, new PeProvisionerSimple(mips)));

		//Another list, for a dual-core machine
		List<Pe> peList2 = new ArrayList<Pe>();

		peList2.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(4, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(7, new PeProvisionerSimple(mips)));
		
		List<Pe> peList3 = new ArrayList<Pe>();

		peList3.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(4, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(7, new PeProvisionerSimple(mips)));
		
		List<Pe> peList4 = new ArrayList<Pe>();

		peList4.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(4, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(7, new PeProvisionerSimple(mips)));
		
		List<Pe> peList5 = new ArrayList<Pe>();

		peList5.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(4, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(7, new PeProvisionerSimple(mips)));
		
		List<Pe> peList6 = new ArrayList<Pe>();

		peList6.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(4, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(7, new PeProvisionerSimple(mips)));

		//4. Create Hosts with its id and list of PEs and add them to the list of machines
		/*int hostId=0;
		int ram = 16384; //host memory (MB)
		long storage = 1000000; //host storage
		int bw = 10000;*/
		
		prop.setProperty("datacenterMips", "8000");
		prop.setProperty("datacenterHostId", "0");
		prop.setProperty("datacenterRam", "131072"); //host memory 128GB
		prop.setProperty("datacenterStorage", "1048576"); //1TB
		prop.setProperty("datacenterBw", "131072"); //128GB
		
		String storageStr = prop.getProperty("datacenterStorage", "1048576");
		String ramStr = prop.getProperty("datacenterRam", "131072");
		String bwStr = prop.getProperty("datacenterBw", "131072"); //16GB
		String hostIdStr = prop.getProperty("datacenterHostId", "0");
		
		long storage = Long.valueOf(storageStr);
		int ram = Integer.valueOf(ramStr);
		long bw = Long.valueOf(bwStr);
		int hostId = Integer.valueOf(hostIdStr);
		

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList1,
    				new VmSchedulerTimeShared(peList1)
    			)
    		); // This is our first machine

		hostId++;

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList2,
    				new VmSchedulerTimeShared(peList2)
    			)
    		); // Second machine

		hostId++; 
		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList3,
    				new VmSchedulerTimeShared(peList3)
    			)
    		);
		
		hostId++; 
		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList4,
    				new VmSchedulerTimeShared(peList4)
    			)
    		);
		
		hostId++; 
		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList5,
    				new VmSchedulerTimeShared(peList5)
    			)
    		);
		
		hostId++; 
		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList6,
    				new VmSchedulerTimeShared(peList6)
    			)
    		);
		

		// 5. Create a DatacenterCharacteristics object that stores the
		//    properties of a data center: architecture, OS, list of
		//    Machines, allocation policy: time- or space-shared, time zone
		//    and its price (G$/Pe time unit).
		String arch = "x86";      // system architecture
		String os = "Linux";          // operating system
		String vmm = "Xeon";
		
		String vmCostStr = prop.getProperty("vmCostPerSec", "0.0000036");
		String storageCostStr = prop.getProperty("storageCostPerGb", "0.03");
		
		double cost = Double.valueOf(vmCostStr);
		double costPerStorage = Double.valueOf(storageCostStr);
		
		double time_zone = 10.0;         // time zone this resource located
		//double cost = 0.013/3600;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		//double costPerStorage = 0.1;	// the cost of using storage in this resource
		double costPerBw = 0.1;			// the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

		/*DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);*/
		
	   characteristics = new DatacenterCharacteristics(arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


		// 6. Finally, we need to create a PowerDatacenter object.
		TranscodingDatacenter datacenter = null;
		try {
			datacenter = new TranscodingDatacenter(name, propertiesFileURL, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	//We strongly encourage users to develop their own broker policies, to submit vms and cloudlets according
	//to the specific rules of the simulated scenario
	private static TranscodingBroker createBroker(){

		TranscodingBroker broker = null;
		try {
			broker = new TranscodingBroker("TranscodingBroker", characteristics, propertiesFileURL);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
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
	/**
	 * Print out each video's start up time and average startup time
	 * @param videoStartupTimeMap
	 * @throws IOException 
	 */
	
	private static void printVideoStatistic(Map<Integer, Double> videoStartupTimeMap, List<Binary> list) throws IOException{
		/*
		System.out.println("\n");

		double videoStartupTimeAverage=0;
		int videoCount =0;
		int size = list.size();
		int deadlineMeetCount = 0;
		double totalUtilityGain = 0;
		double utilityRate = 0.0;
		
		for(Binary cl:list){
		    if(cl.getCloudletDeadline() > cl.getFinishTime()){
		    	totalUtilityGain += cl.getUtilityNum();
		    	deadlineMeetCount++;
		    }
		    
		}
		
		utilityRate = totalUtilityGain/deadlineMeetCount;
		
		Binary cloudlet;
		Map<Integer, Integer> videoGopNumMap = new HashMap<Integer,Integer>();
		Map<Integer, Integer> videoDeadlineMissNumMap = new HashMap<Integer, Integer>();
		Map<Integer, Double> videoDeadlineMissRateMap = new HashMap<Integer, Double>();
		Map<Integer, Double> videoBufferDelayMap = new HashMap<Integer, Double>();
		Map<Integer, Integer> gopOrderNumMap = new HashMap<Integer, Integer>();
		
		int deadlineMissCount = 0;

		for(Binary cl:list){
			if(!videoDeadlineMissNumMap.containsKey(cl.getCloudletVideoId())){
				
				videoGopNumMap.put(cl.getCloudletVideoId(), 1);
				videoDeadlineMissNumMap.put(cl.getCloudletVideoId(), 0);
				if(cl.getCloudletDeadline() < cl.getFinishTime()){
					videoDeadlineMissNumMap.put(cl.getCloudletVideoId(), videoDeadlineMissNumMap.get(cl.getCloudletVideoId())+1);
			    }

			}else{
				videoGopNumMap.put(cl.getCloudletVideoId(), videoGopNumMap.get(cl.getCloudletVideoId())+1);
				if(cl.getCloudletDeadline() < cl.getFinishTime()){
					videoDeadlineMissNumMap.put(cl.getCloudletVideoId(), videoDeadlineMissNumMap.get(cl.getCloudletVideoId())+1);
			    }
			}
			
		  			
		}
		
		for(Map.Entry<Integer, Integer> entry:videoGopNumMap.entrySet()){
			double videoDeadlineMissNum = videoDeadlineMissNumMap.get(entry.getKey());
			double videoGopNum = videoGopNumMap.get(entry.getKey());
			double videoDeadlineMissRate = videoDeadlineMissNum/videoGopNum;
			videoDeadlineMissRateMap.put(entry.getKey(), videoDeadlineMissRate);
		}
		
		for(Binary vsg:list){
					
			if(!videoBufferDelayMap.containsKey(vsg.getCloudletId())){
				
				videoBufferDelayMap.put(vsg.getCloudletId(), vsg.getFinishTime()-vsg.getArrivalTime());
				gopOrderNumMap.put(vsg.getCloudletId(), 1);

			}else{
				videoBufferDelayMap.put(vsg.getCloudletId(), (videoBufferDelayMap.get(vsg.getCloudletId()) + vsg.getFinishTime()-vsg.getArrivalTime()));
				gopOrderNumMap.put(vsg.getCloudletId(), (gopOrderNumMap.get(vsg.getCloudletId()) + 1));
			}		
		  			
		}
		
		//System.out.println("\nThe average GOP buffer delay is:\n");
		
		PrintWriter pw2 = new PrintWriter(new FileWriter(gopDelayOutputURL, true));
		
		for(Integer key:videoBufferDelayMap.keySet()){
			if(key == 0){
				pw2.printf("%-18s%-18s%-20s", "Scheduling", "GOP#", "StartupTime");
				pw2.println("\n");
				pw2.printf("%-18s%-18d%-20.2f", prop.getProperty("schedulingmethod", "MM"), key, videoBufferDelayMap.get(key)/gopOrderNumMap.get(key));
				pw2.println("\n");
			}else{
				
				pw2.printf("%-18s%-18d%-20.2f", prop.getProperty("schedulingmethod", "MM"), key, videoBufferDelayMap.get(key)/gopOrderNumMap.get(key));
				pw2.println("\n");
			}
			
		}
		
		pw2.close();
		

		
		//PrintWriter pw = new PrintWriter("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudTranscodingApp/resources/outputdata/video_startup_time.txt");
		PrintWriter pw = new PrintWriter(new FileWriter(outputdataFileURL, true));
		//pw.printf("%-16s%-16s%-25s", "VideoID", "Startup Time", "Deadline Miss Rate");
		//pw.println("\n");
		
		for(Integer vId:videoStartupTimeMap.keySet()){
		//	System.out.println("VideoId#" + vId + "'s start up time is: " + new DecimalFormat("#0.00").format(videoStartupTimeMap.get(vId)));
			videoStartupTimeAverage += videoStartupTimeMap.get(vId);
			videoCount++;
		//	pw.printf("%-16d%-16.2f%-25.4f", vId, videoStartupTimeMap.get(vId), videoDeadlineMissRateMap.get(vId));
		//	pw.println("\n");

			
		}
		
		//pw.close();
		double totalAverageStartupTime = videoStartupTimeAverage/videoCount;
		System.out.println("Video average start up time is: "+ new DecimalFormat("#0.0000").format(totalAverageStartupTime));
		
		double deadlineMissNum = 0;
		double totalDeadlineMissRate;
		
		for(Binary cl:list){
		    if(cl.getCloudletDeadline() < cl.getFinishTime()){
		    	deadlineMissNum++;
		    }
		}
		
		totalDeadlineMissRate = deadlineMissNum/list.size();
		
		if(startupqueue){
		
	
			if(vmNum == 0){
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25s%-20s%-12s%-12s%-12s%-12s", "Startup Queue", "Scheduling", "Video Numbers", "VM Number ", "VQS", "Average Startup Time", "Deadline Miss Rate", "Total Cost", "UtilityGain", "DMR_UPTH", "DMR_LTH");
				pw.println("\n");
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25.2f%-20.4f%-12.4f%-12.4f%-12.4f%-12.4f", "YES", prop.getProperty("schedulingmethod", "MM"), prop.getProperty("periodEventNum", "2"), "Dynamic", prop.getProperty("waitinglist_max", "2"), totalAverageStartupTime, totalDeadlineMissRate, totalCost, utilityRate, Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_UPTH", "0.10")), Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_LOWTH", "0.05")));
				
				pw.println("\n");
			}else{
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25s%-20s%-12s%-12s%-12s%-12s", "Startup Queue", "Scheduling", "Video Numbers", "VM Number ", "VQS", "Average Startup Time", "Deadline Miss Rate", "Total Cost", "UtilityGain", "DMR_UPTH", "DMR_LTH");
				pw.println("\n");
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25.2f%-20.4f%-12.4f%-12.4f%-12.4f%-12.4f", "YES", prop.getProperty("schedulingmethod", "MM"), prop.getProperty("periodEventNum", "2"), prop.getProperty("vmNum", "2"), prop.getProperty("waitinglist_max", "2"), totalAverageStartupTime, totalDeadlineMissRate, totalCost, utilityRate, Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_UPTH", "0.10")), Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_LOWTH", "0.05")));
				
				pw.println("\n");
				
			}
		
		}else{

			
			if(vmNum == 0){
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25s%-20s%-12s%-12s%-12s%-12s", "Startup Queue", "Scheduling", "Video Numbers", "VM Number ", "VQS", "Average Startup Time", "Deadline Miss Rate", "Total Cost", "UtilityGain", "DMR_UPTH", "DMR_LTH");
				pw.println("\n");
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25.2f%-20.4f%-12.4f%-12.4f%-12.4f%-12.4f", "NO", prop.getProperty("schedulingmethod", "MM"), prop.getProperty("periodEventNum", "2"), "Dynamic", prop.getProperty("waitinglist_max", "2"), totalAverageStartupTime, totalDeadlineMissRate, totalCost, utilityRate, Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_UPTH", "0.10")), Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_LOWTH", "0.05")));
				
				pw.println("\n");
			}else{
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25s%-20s%-12s%-12s%-12s%-12s", "Startup Queue", "Sorting", "Video Numbers", "VM Number ", "VQS", "Average Startup Time", "Deadline Miss Rate", "Total Cost", "UtilityGain", "DMR_UPTH", "DMR_LTH");
				pw.println("\n");
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25.2f%-20.4f%-12.4f%-12.4f%-12.4f%-12.4f", "NO", prop.getProperty("schedulingmethod", "MM"), prop.getProperty("periodEventNum", "2"), prop.getProperty("vmNum", "2"), prop.getProperty("waitinglist_max", "2"), totalAverageStartupTime, totalDeadlineMissRate, totalCost, utilityRate, Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_UPTH", "0.10")), Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_LOWTH", "0.05")));
				
				pw.println("\n");
				
			}
			
		}
		
		pw.close();
		*/
	}

	/**
	 * Prints the Cloudlet objects
	 * @param list  list of Cloudlets
	 * @throws Exception 
	 */
	private static void printCloudletList(List<Binary> list) throws Exception {
		int size = list.size();
		Binary cloudlet;
		int deadlineMissCount = 0;	
		double totalDeadlineMissRate;
		
		int deadlineMeetCount = 0;
		double totalUtilityGain = 0;
		double utilityRate = 0.0;
		/*
		for(Binary cl:list){
		    if(cl.getCloudletDeadline() < cl.getFinishTime()){
		    	deadlineMissCount++;
		    }else{
		    	totalUtilityGain += cl.getUtilityNum();
		    	deadlineMeetCount++;
		    }
		    
		}
		*/
		utilityRate = totalUtilityGain/deadlineMeetCount;
		
		/*System.out.println("\nThere are: " + list.size() + " cloudlets...");
		System.out.println("\nThere are: " + deadlineMissCount + " cloudlets missed deadline...");*/
		
		totalDeadlineMissRate = (double) deadlineMissCount/list.size();
        
		System.out.println("\nThe total deadline miss rate is: " + new DecimalFormat("#0.0000").format(totalDeadlineMissRate));
		System.out.println("\nThe total utilty gain is: " + new DecimalFormat("#0.0000").format(totalUtilityGain));
		System.out.println("\nThe utilty rate is: " + new DecimalFormat("#0.0000").format(utilityRate));

		
		
		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		/*Log.printLine("Video ID" + indent +
				      "Cloudlet ID" + indent + "STATUS" + indent + "Data center ID" + indent + 
				      "VM ID" + indent + indent + 
				      "Arrival Time"+ indent + indent + 
				      "Start Exec Time" + indent + indent +
				      "Exec Time" + indent  + indent + 
				      "Finish Time" + indent  + indent +
				      "Deadline");*/
        System.out.format("%-18s%-18s%-18s%-18s%-18s%-18s%-18s%-18s%-18s%-18s", "Video ID","Cloudlet ID",
        		"STATUS", "Data center ID", "VM ID", "Arrival Time",  "Start Exec Time", "Exec Time","Finish Time","Deadline");
        System.out.println("\n");
		
		//DecimalFormat dft = new DecimalFormat("###.##");
		DecimalFormat dft = new DecimalFormat("###");
		for (int i = 0; i < size; i++) {
			cloudlet = (Binary) list.get(i);
		//	Log.print(indent + cloudlet.getCloudletVideoId() + indent + indent + indent + cloudlet.getCloudletId() + indent + indent);
			
			if (cloudlet.getCloudletStatus() == Binary.SUCCESS){
				/*Log.print("SUCCESS");

				Log.printLine( 
						indent + indent + cloudlet.getResourceId() + 
						indent + indent + indent + indent + cloudlet.getVmId() + 
						indent + indent + indent + indent + dft.format(cloudlet.getArrivalTime()) + 
						indent + indent + indent + indent + dft.format(cloudlet.getExecStartTime()) +
						indent + indent + indent + indent + indent + dft.format(cloudlet.getActualCPUTime()) +
					    indent + indent + indent + indent + dft.format(cloudlet.getFinishTime()) +
					    indent + indent + indent + indent + dft.format(cloudlet.getCloudletDeadline()));*/
				
	           // System.out.format("%-18d%-18d%-18s%-18d%-18d%-18.2f%-18.2f%-18.2f%-18.2f%-18.2f", cloudlet.getCloudletVideoId(), cloudlet.getCloudletId(), "SUCCESS", 
	          //  		cloudlet.getResourceId(), cloudlet.getVmId(), cloudlet.getArrivalTime(), cloudlet.getExecStartTime(), cloudlet.getActualCPUTime(),
	            //		cloudlet.getFinishTime(),cloudlet.getCloudletDeadline());

			}
		}
		
		System.out.println("There are " + size + " cloudlets are processed.");

	}
	
	
	/**
	 * Coordinator class is used to control the whole cloud system:
	 * 1. Reading New Video streams
	 * 2. Split Video Streams
	 * 3. Create Broker
	 * 4. Based VMProvision allocate and deallocate VMs.
	 * 
	 * @author Bill
	 *
	 */
	
	public class Coordinator extends SimEntity {
	    
		private static final int CREATE_BROKER = 150;
	 
		private static final int PERIODIC_UPDATE = 153;
		
		private static final int CREATE_JOB = 155;
		
		private static final int DROP_VIDEO = 128;
		
		private static final int CHECK_UTILIZATION = 130;
		
		private static final int CREATE_VM_TYPE = 133;
		
		private int MAX_VM_NUM;
		
		
		
		public int videoId = 0;
		
		private int periodCount = 0;
		
		//Max jobs we are about to create
		private int periodEventNum;
		

		
		private int periodicDelay;
	 //   public boolean generatePeriodicEvent = true; //true if new internal events have to be generated

		
		private List<TranscodingVm> vmList;
		private List<Binary> cloudletList;
		private TranscodingBroker broker;
		
	    private int jobCount = 0;
	    private int previousJobCount = 0;
		private double jobDelay = 0.0;
		private int vmNumber = 0;
		private String clusterType = null;
		private String staticVmType = null;
		private int seedShift = 0;
		private String estimatedGopLength = null;
		private double DEADLINE_MISS_RATE_UPTH = 0.0;
		private double DEADLINE_MISS_RATE_LOWTH = 0.0;
		private double testPeriod = 0.0;
		private long rentingTime = 0;
		private double utilizationCheckPeriod = 1000;
		
		private boolean prediction = true;
		private boolean stqprediction = true;
		private boolean startupqueue = true;
		private boolean dropflag = true;
		
		private CSVReader reader;
		private CSVReader lookahead;
		//private String [] nextLine;


		Map<TranscodingVm, Double> cpuUtilizationMap = new HashMap<TranscodingVm, Double>();
		int cupUtilizationPeriodCount = 0;
		

		public Coordinator(String name) throws IOException {
			super(name);

			InputStream input = new FileInputStream(propertiesFileURL);
			prop.load(input);
			
			String jobNum;
			String frequence;
			String MAX_VM_NUM;
			String vmNumber;
			String seedShift;
			String upTh; 
			String lowTh;
			String testPeriod;
			String rentingTime;
			String stqprediction;
			String startupqueue;
			String dropflag;
			String staticVmType;
			String clusterType;
			CSVReader reader;
			
			
			
			jobNum = prop.getProperty("periodEventNum");
			this.periodEventNum = Integer.valueOf(jobNum);
			
			frequence = prop.getProperty("periodicDelay");
			this.periodicDelay = Integer.valueOf(frequence);
			
			MAX_VM_NUM = prop.getProperty("MAX_VM_NUM", "16");
			this.MAX_VM_NUM = Integer.valueOf(MAX_VM_NUM);
			
			vmNumber = prop.getProperty("vmNum");
			this.vmNumber = Integer.valueOf(vmNumber);
			
			clusterType = prop.getProperty("clusterType");
			this.clusterType = clusterType;
			
		    staticVmType = prop.getProperty("vmType");
		    this.staticVmType = staticVmType;
			
			seedShift = prop.getProperty("seedShift");
			this.seedShift = Integer.valueOf(seedShift);
			
		    upTh = prop.getProperty("DEADLINE_MISS_RATE_UPTH", "0.2");
			this.DEADLINE_MISS_RATE_UPTH = Double.valueOf(upTh);
			
			lowTh = prop.getProperty("DEADLINE_MISS_RATE_LOWTH", "0.01");
			this.DEADLINE_MISS_RATE_LOWTH = Double.valueOf(lowTh);
			
			testPeriod = prop.getProperty("testPeriod", "1800000");
			this.testPeriod = Double.valueOf(testPeriod);
			
			rentingTime = prop.getProperty("rentingTime", "60000");
			this.rentingTime = Long.valueOf(rentingTime);
			
			stqprediction = prop.getProperty("stqprediction", "true");
			this.stqprediction = Boolean.valueOf(stqprediction);
			
			startupqueue = prop.getProperty("startupqueue", "true");
			this.startupqueue = Boolean.valueOf(startupqueue);

			dropflag = prop.getProperty("dropflag", "true");
			this.dropflag = Boolean.valueOf(dropflag);

			
			estimatedGopLength = prop.getProperty("estimatedGopLength");
			
		     try {
					this.reader = new CSVReader(new FileReader(inputdataFolderURL));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			     try {
					this.reader.readNext();//Parse the header
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} //Header
			     
			     
			     
			     try {
						this.lookahead = new CSVReader(new FileReader(inputdataFolderURL));
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				     try {
						this.lookahead.readNext();//Parse the header
						this.lookahead.readNext();//Parse the first record
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} //Header
			
		}

		@Override
		public void processEvent(SimEvent ev) {
			switch (ev.getTag()) {
			case CREATE_BROKER:
			    setBroker(createBroker());
			    //create and submit intial vm list
			    
			    if(vmNum == 0){
			    	Log.printLine("Creating dynamic VMs");
			        setVmList(createVM(broker.getId(), "g2.2xlarge", 1, 0, Long.MAX_VALUE));
			        broker.submitVmList(getVmList());

			    }else if(!startupqueue){
			        
			    	
			    				    		
			    		if(clusterType.equals("homogeneous")){
			    			for(int i=0; i<vmNum; i++){

				    		setVmList(createVM(broker.getId(), staticVmType, 1, i, Long.MAX_VALUE));
	                        broker.submitVmList(getVmList());
	                        
			    			}
			    		}else{
			    			Log.printLine("Creating heterogeneous VMs");
			    			for(int i=0; i<4; i++){

					    		setVmList(createVM(broker.getId(), "g2.2xlarge", 1, i, Long.MAX_VALUE));
		                        broker.submitVmList(getVmList());
		                        
				    	    }
			    			
			    			for(int i=4; i<8; i++){

					    		setVmList(createVM(broker.getId(), "c4.xlarge", 1, i, Long.MAX_VALUE));
		                        broker.submitVmList(getVmList());
		                        
				    	    }
			    			
			    			for(int i=8; i<10; i++){

					    		setVmList(createVM(broker.getId(), "r3.xlarge", 1, i, Long.MAX_VALUE));
		                        broker.submitVmList(getVmList());
		                        
				    	    }
			    			
			    			for(int i=10; i<12; i++){

					    		setVmList(createVM(broker.getId(), "m4.large", 1, i, Long.MAX_VALUE));
		                        broker.submitVmList(getVmList());
		                        
				    	    }
			    			
			    			/*for(int i=12; i<16; i++){

					    		setVmList(createVM(broker.getId(), "m4.large", 1, i, Long.MAX_VALUE));
		                        broker.submitVmList(getVmList());
		                        
				    	    }*/
			    			
			    			

			    		
			    	    }
			    	

			    }else{    		
			    		
				        setVmList(createVM(broker.getId(), "c4.xlarge", vmNum, 0, Long.MAX_VALUE));
				        broker.submitVmList(getVmList());
			    	

			    }
			     
				//broker.submitVmList(getVmList());
			    
				break;
			
			case CREATE_JOB:
				processNewJobs();
				break;
		
			case PERIODIC_UPDATE:				
				processProvisioning();
                break;
                
            	// drop videos
			case DROP_VIDEO:
				dropVideo();
				break;
				
			case CHECK_UTILIZATION:
				checkCpuUtilization();
				break;
			
			default:
				Log.printLine(getName() + ": unknown event type");
				break;
			}
		}

		@Override
		/**
		 * Open video stream arrival rate file and scheduling the initial tasks of creates jobs and VMs
		 */
		public void startEntity() {
			Log.printLine(getName()+" is starting...");
			schedule(getId(), 0, CREATE_BROKER);
			schedule(getId(), 1, CREATE_JOB);
			
			if(dropflag){
				   schedule(getId(), 2, DROP_VIDEO);
				}
			
			send(getId(), utilizationCheckPeriod, CHECK_UTILIZATION);	
			send(getId(), periodicDelay, PERIODIC_UPDATE);
		}

		@Override
		/**
		 * Close all the files readers that has been opened in the simulation
		 */
		public void shutdownEntity() {
			Log.printLine(getName()+" is shuting down...");

			
		}
		
		/**
		 * Periodically check CPU utilization
		 */
		public void checkCpuUtilization() {
			
			double currentCpuUtilization = 0;
			double totalCupUtilization = 0;
			
			
			//cupUtilizationPeriodCount++;
			
			for(Vm vm:broker.getVmList()){
				TranscodingVm tvm = (TranscodingVm)vm;
				
				currentCpuUtilization = tvm.getTotalUtilizationOfCpu(CloudSim.clock());
				
				if(cpuUtilizationMap.containsKey(tvm)){
				   totalCupUtilization = cpuUtilizationMap.get(tvm) + currentCpuUtilization;				
				   cpuUtilizationMap.put(tvm, totalCupUtilization);
				//   tvm.setPeriodicUtilizationRate(totalCupUtilization/cupUtilizationPeriodCount);
				}else{
			       cpuUtilizationMap.put(tvm, currentCpuUtilization);
			      // tvm.setPeriodicUtilizationRate(totalCupUtilization/cupUtilizationPeriodCount);
				}
				
				//System.out.println("***************The CPU current utilization of " + tvm.getVmType() + " is: " +tvm.getTotalUtilizationOfCpu(CloudSim.clock()) + " ***************");
			    
			}
			if (broker.getCloudletList().size() == 0 && broker.cloudletSubmittedCount == 0 && jobCount == periodEventNum) {
			    return;
			}else{
				send(getId(),utilizationCheckPeriod,CHECK_UTILIZATION);	

		    }
			
			
					
		}
		
		/**
		 * Periodically process new jobs
		 * @throws ParseException 
		 */
		
		public void processNewJobs() {
				System.out.println(CloudSim.clock() + " : Creating a new job....");
				
				int brokerId = getBroker().getId();
				
				
				// Create a thread pool that can create video streams
				
		 		Random random = new Random();
	
				//List<Cloudlet> newList = new ArrayList<Cloudlet>();
	            
		 		
		 		
		 		
				ExecutorService executorService = Executors.newFixedThreadPool(2);
				 
		        CompletionService<String> taskCompletionService = new ExecutorCompletionService<String>(
		                executorService);
		        String [] nextLine;
		        long timeInMillis = 0;
		        
		        try {
		        	
		        	ArrayList<Callable<String>> callables = new ArrayList<Callable<String>>();
		            for (int i = 0; i < 1; i++) {
		            	int cloudlets = (int)getRandomNumber(10, 50, random);
		            	System.out.println("Creating a binary stream of cloudlets for " + videoId );
		       	    
		    	     try {
		    			nextLine = reader.readNext();
		    		} catch (IOException e) {
		    			// TODO Auto-generated catch block
		    			e.printStackTrace();
		    			return;
		    		} 
		            	  //callables.add(new VideoStreams("" + videoId, inputdataFolderURL, startupqueue, estimatedGopLength, seedShift, brokerId, videoId, cloudlets));
		    	     SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		    	     try{
		    	    	 Date d = df1.parse(nextLine[0]);
		    	    	 timeInMillis = d.getTime();
		    	    	 System.out.println("Time in ms = "+timeInMillis);
		    	     }
		    	     catch(Exception e){
		    	    	 System.out.println("Couldn't parse, chnage code!!!");
		    	     }
		            	  callables.add(new BinaryStream("" + videoId, inputdataFolderURL, startupqueue, estimatedGopLength, seedShift, brokerId, videoId, cloudlets, nextLine));
		            	  //Don't know what this is doing or where there is called
		            	  System.out.println(videoId + inputdataFolderURL + startupqueue + estimatedGopLength + seedShift+ brokerId + videoId + cloudlets);
			           // Thread.sleep(1000);
		                videoId++;
		            }
		            
		            
		            
		            
		           // List<Callable<String>> callables = createCallableList();
		            for (Callable<String> callable : callables) {
		                taskCompletionService.submit(callable);     
		               // Thread.sleep(500);
		            }
		            
		           // Thread.sleep(1000);
						            
		            
		            for (int i = 0; i < callables.size(); i++) {
		                Future<String> result = taskCompletionService.take(); 
		                System.out.println(result.get() + " End."); 
		            }
		        } catch (InterruptedException e) {
		            // no real error handling. Don't do this in production!
		            e.printStackTrace();
		        } catch (ExecutionException e) {
		            // no real error handling. Don't do this in production!
		            e.printStackTrace();
		        }
		        executorService.shutdown();
		      
			    //VideoStreams vt = new VideoStreams();
		        BinaryStream vt = new BinaryStream();
			    cloudletBatchQueue = vt.getBatchQueue();
			    cloudletNewArrivalQueue = vt.getNewArrivalQueue();
			    System.out.println("Size of new arrival queue " + cloudletNewArrivalQueue.size());
			
			    //update the cloudlet list and back to simulation
			    broker.submitCloudletList(cloudletBatchQueue, cloudletNewArrivalQueue);

			    

			 
			    broker.submitCloudlets();
			     
			//Check if there are more jobs to process
			//Add Delay before other files are processed
			    
		    if (broker.generatePeriodicEvent){
		    	System.out.println("Checking where this is called");
			    jobCount++;
			    if(jobCount < periodEventNum){
			    	Random r = new Random(jobCount);
			    	//It should be the difference between upload times, include delay = diff of upload times of this and next record
			    	/*
			    	jobDelay = testPeriod/periodEventNum;

			    	double val = r.nextGaussian()*(jobDelay/3);
			    	double stdJobDelay = val + jobDelay;
			    	System.out.println("stdJobDelay " + stdJobDelay);
			    	*/
			    	
			    	
		       	    String [] nextJob;
		       	 long next_timeInMillis;
		       	 double stdJobDelay = 0;
		    	     try {
		    			nextJob = lookahead.readNext();
		    		} catch (IOException e) {
		    			// TODO Auto-generated catch block
		    			e.printStackTrace();
		    			return;
		    		} 
		            	  //callables.add(new VideoStreams("" + videoId, inputdataFolderURL, startupqueue, estimatedGopLength, seedShift, brokerId, videoId, cloudlets));
		    	     SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		    	     try{
		    	    	 Date d = df1.parse(nextJob[0]);
		    	    	next_timeInMillis = d.getTime();
		    	    	 System.out.println("Time in ms for next = "+ next_timeInMillis);
		    	    	 stdJobDelay = (next_timeInMillis - timeInMillis);    ///1000000;
		    	    	 System.out.println("Std Dev in s = " + stdJobDelay);
		    	     }
		    	     catch(Exception e){
		    	    	 System.out.println("Couldn't parse, chnage code!!!");
		    	     }
			    	
			    	
			    	
					send(getId(),stdJobDelay,CREATE_JOB); //Not sure if the delay is in sec or ms; clarify this
					
					
			    	
			    }else{
			    	broker.generatePeriodicEvent = false;	
	
			    }
		     }   
			   
			    CloudSim.resumeSimulation();
			
	 
		}
		
		
		/**
		 * Periodically drop video
		 * 
		 */

		private void dropVideo() {

			/*
			List<Integer> videoDropList = new ArrayList<Integer>();
			double estdmr = 0.0;
			try {
				TranscodingProvisioner trp = new TranscodingProvisioner(broker, propertiesFileURL);
			    //estdmr = trp.getEstimatedDeadlineMissRate();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			//
			for (int i = 0; i < broker.getCloudletList().size(); i++) {
				Binary vs = (Binary) broker.getCloudletList().get(i);
				if (vs.getOrderNum() == 0) {

					Random random = new Random();
					double cmp = random.nextDouble();
					boolean drop;
					if (vs.getUtilityNum() < cmp) {
						drop = true;
					} else {
						drop = false;
					}
					// boolean drop = random.nextBoolean();

					 System.out.println("<--------The drop flag of "
					 +vs.getCloudletVideoId() +" is: " + drop + " -------->" +
					 vs.getUtilityNum());

					if (!videoDropList.contains(vs.getCloudletVideoId()) && drop) {

						videoDropList.add(vs.getCloudletVideoId());
						// System.out.println("Video #" + vs.getCloudletVideoId() +
						// " is about to be dropped");
					} else {
						continue;
					}
				}
			}

			for (int i = 0; i < broker.getCloudletList().size(); i++) {
				Binary vst = (Binary) broker.getCloudletList().get(i);

				if (videoDropList.contains(vst.getCloudletVideoId())) {

					broker.getCloudletList().remove(vst);
					i--;
					 System.out.println("Video #" + vst.getCloudletVideoId() +
					 " Cloudlet #" + vst.getCloudletId() + " has been dropped");

				} else {
					continue;
				}
			}

			// Gaussian dissdribution of droping videos
			Random r = new Random();
			double averageDropPeriod = 10000;

			double eventDelay = r.nextGaussian() * averageDropPeriod / 2
					+ averageDropPeriod;

			if (broker.getCloudletList().size() > 0 || broker.generatePeriodicEvent) {
				send(getId(), eventDelay, DROP_VIDEO);
			}*/

		}
		
		
		/**
		 * Periodically check Resource Provisioning,and update VMs 
		 */
		
		public void processProvisioning(){
			/**
			 * Dynamic Vm policy
			 * 
			 */
			
			
			if(vmNum == 0){
				/**
				 * no prediction mode
				 */
	 
				System.out.println("\n" + CloudSim.clock() + ": " + "Upating Transcoding provisioning...");
				double cpuUtilizationRate;
				double highestUtilizationRate = 0.0;
				TranscodingVm highestUtilizationRateVm = null;
				Map<String, Double> vmTypeTotalUtilizationMap = new HashMap<String, Double>();
				Map<String, Integer> vmTypeNumberMap = new HashMap<String, Integer>();
				Map<String, Double> vmTypeMinUtizationMap = new HashMap<String,Double>();
				double totalVmTypeUtilizationRate = 0.0;
				
				
				for(Vm vm : datacenter.getVmList()){
					TranscodingVm vmm = (TranscodingVm) vm;
				
					String vmType;
					int vmTypeNumber;
					double vmTypeMinUtilizationRate ;
					
					if(!cpuUtilizationMap.containsKey(vmm)){
						cpuUtilizationRate = 0.0;
						vmm.setPeriodicUtilizationRate(cpuUtilizationRate);
					}else{
						cpuUtilizationRate = cpuUtilizationMap.get(vmm)/(periodicDelay/utilizationCheckPeriod);					
						vmm.setPeriodicUtilizationRate(cpuUtilizationRate);
					}
					
					vmType = vmm.getVmType();				
					
					if(!vmTypeTotalUtilizationMap.containsKey(vmType)){
					   totalVmTypeUtilizationRate = cpuUtilizationRate;
					   vmTypeNumberMap.put(vmType, 1);
					   vmTypeTotalUtilizationMap.put(vmType, totalVmTypeUtilizationRate);

					   vmTypeMinUtilizationRate = cpuUtilizationRate;
					   vmTypeMinUtizationMap.put(vmType, vmTypeMinUtilizationRate);


					}else{
					   totalVmTypeUtilizationRate += vmTypeTotalUtilizationMap.get(vmType);
					   vmTypeTotalUtilizationMap.put(vmType, totalVmTypeUtilizationRate);
					   vmTypeNumber = vmTypeNumberMap.get(vmType) + 1;
					   vmTypeNumberMap.put(vmType, vmTypeNumber);   
					   
					   if(cpuUtilizationRate < vmTypeMinUtizationMap.get(vmType)){
						   vmTypeMinUtilizationRate = cpuUtilizationRate;
						   vmTypeMinUtizationMap.put(vmType, vmTypeMinUtilizationRate);
					   }
					   
						
					}
					
					
					if(cpuUtilizationRate > highestUtilizationRate){
						highestUtilizationRate = cpuUtilizationRate;
						highestUtilizationRateVm = vmm;
					}

					System.out.println(CloudSim.clock() + "***************The total CPU utilization rate of VM" + vmm.getId() + "(" + vmm.getVmType() + ")" + " is: " + cpuUtilizationRate + " ***************");
				}
				
				System.out.println("\n");
				
				
				TranscodingProvisioner tper;
			   // int allcoationDeallocationFlag = 0;
				try {
					tper = new TranscodingProvisioner(broker, propertiesFileURL);									
					//allcoationDeallocationFlag = tper.allocateDeallocateVm();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				//when a vm's remaining time is lessing than 0, either recharge it or remove it based it's deallocation flag 
				int runningVmNum = 0;	
				double vmUtilizationRate;
				
				for(Vm vm : datacenter.getVmList()){
					TranscodingVm vmm = (TranscodingVm) vm;
					VideoSchedulerSpaceShared vmcsc = (VideoSchedulerSpaceShared) vmm.getCloudletScheduler();
                    System.out.println("*********The remaining time of VM" + vmm.getId() + " is " + vmm.getRemainingTime() + "******");
					
                    
					if(vmm.getRemainingTime() >= 10){
						runningVmNum ++;
					}else if(vmm.getRemainingTime()<= 10 && !vmm.getDeallocationFlag()){
						
						vmUtilizationRate = cpuUtilizationMap.get(vmm)/(periodicDelay/utilizationCheckPeriod);
						
						if(vmUtilizationRate > 0.2){
						    vmm.setRentingTime(rentingTime);
						    System.out.println("     ****** VM" +vmm.getId() + " has been recharged from resource provisiong *****");
						}else if(vmUtilizationRate <= 0.2){
							   
							    /*TranscodingProvisioner tp;
							    double weight;
								double highestWeight = 0.0;
							    String vmType = null;
							    double alpha = 0.8;
							    
							    try {
									 tp = new TranscodingProvisioner(broker, propertiesFileURL);
									 
									 Map<String, Double> missedDeadlineGopTypeMap = new HashMap<String, Double>(tp.getMissedDeadlineGopTypeMap());
									 Map<String, Double> batchGopTypeMap = new HashMap<String, Double>(tp.getBatchGopTypeMap());
									 
									 
									 for(String key:batchGopTypeMap.keySet()){
										    
										   weight = alpha*missedDeadlineGopTypeMap.get(key) + (1-alpha)*batchGopTypeMap.get(key);
										   
										   if(weight > highestWeight){
											   highestWeight = weight;
											   vmType = key;
										   }
										   
									  }		 								 
									 
									 
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							    
							   
								
							    
							    if(highestUtilizationRate > 0.5 && highestWeight > 0.1){
							        System.out.println("\n****** VM" +vmm.getId() + " renting time expired and utilization is too low, therefore transform if to high needs VM type: " + vmType + " from resource provision*****");
                                    provisionVM(1, vmType); 
							    }else{
 
						            System.out.println("\n****** VM" +vmm.getId() + " renting time expired and utilization is too low, therefore set deallocation flag and remove it from resource provision*****");
							    }*/
							    
				                System.out.println("     ****** VM" +vmm.getId() + " renting time expired and utilization is too low, therefore set deallocation flag and remove it from resource provision*****");

								
								vmm.setDeallocationFlag(true);	
								
								VideoSchedulerSpaceShared scheduler = (VideoSchedulerSpaceShared)vmm.getCloudletScheduler();
						    	if(scheduler.getCloudletExecList().size() == 0 && scheduler.getCloudletWaitingList().size() == 0 && scheduler.getCloudletPausedList().size() == 0) {
						    		
						    		System.out.println(CloudSim.clock() + "      ********************Cloudles in VM_" + vmm.getId() + " have finished from resource provisioning***********************" );
						    		sendNow(broker.getVmsToDatacentersMap().get(vmm.getId()), CloudSimTags.VM_DESTROY, vmm);
					                broker.vmDestroyedList.add(vmm);
					                
					                //set Vm's finish time.
					                vmm.setVmFinishTime(CloudSim.clock());
					    	      
					                //Calculate vm cost based on the time it last.
					                broker.setVmCost(vmm);
						    	}
						    	
						    	
                                
                                
					    }
		
					}else if(vmm.getRemainingTime() <= 0 && vmm.getDeallocationFlag() && vmcsc.getCloudletExecList().size() == 0 && vmcsc.getCloudletWaitingList().size() == 0 && vmcsc.getCloudletPausedList().size() == 0){
					    	//System.out.println(CloudSim.clock() + "\n********************VM_" + vmm.getId() + "'s renting time is over and to be destroyed***********************" );
							sendNow(broker.getVmsToDatacentersMap().get(vmm.getId()), CloudSimTags.VM_DESTROY, vmm);
			                broker.vmDestroyedList.add(vmm);
			                
			                //set Vm's finish time.
			                vmm.setVmFinishTime(CloudSim.clock());
			    	        
			                //Calculate vm cost based on the time it last.
			                broker.setVmCost(vmm);
					}
				}
				
				System.out.println("\n");
					
				TranscodingProvisioner tp;
				try {
					tp = new TranscodingProvisioner(broker, propertiesFileURL);
					int allcoationDeallocationFlag;
					allcoationDeallocationFlag = tp.allocateDeallocateVm();
					
					System.out.println("The VMList size is: " + datacenter.getVmList().size());
					System.out.println("The Created VMList size is: " + broker.getVmsCreatedList().size());
	
					//1 means allocate new a vm, 2 means deallocate a vm
					if(allcoationDeallocationFlag == 1 ){
						
						
						if(datacenter.getVmList().size() < MAX_VM_NUM) {
							
							Map<String, Double> missedDeadlineGopTypeMap = new HashMap<String, Double>(tp.getMissedDeadlineGopTypeMap());
							Map<String, Double> batchGopTypeMap = new HashMap<String, Double>(tp.getBatchGopTypeMap());
							Map<String, Double> gopTypeWeightMap = new HashMap<String, Double>();
							//Map<String, Integer> allocateDealocateVmMap = new HashMap<String, Integer>();
							double alpha = 0.3;
							double weight = 0.0;
						    double highestWeight = 0.0;
					   	    String highestWeightGopType = null;
					   	    int allocateVmNum = 1;
					        double arrivalRate = 0.0;
					   	    
					   	    
					   	    for(String key:batchGopTypeMap.keySet()){
							    
							   weight = alpha*missedDeadlineGopTypeMap.get(key) + (1-alpha)*batchGopTypeMap.get(key);
							   
							   System.out.println("****The weight for " + key + " is " + weight + "********");
							   
							   gopTypeWeightMap.put(key, weight);
							   
						    }
						   						   
						   /* for(String key:gopTypeWeightMap.keySet()){
							   if (highestWeight == 0.0 || gopTypeWeightMap.get(key).compareTo(highestWeight) > 0)
							    {
								   highestWeight = gopTypeWeightMap.get(key);
								   highestWeightGopType = key;			  
							    }
						    }*/
					   	    
					   	    
					   	    
					   	    
					   	    arrivalRate = (jobCount - previousJobCount)*1000.0/periodicDelay;
		                	previousJobCount = jobCount;
					   	    
					   	    for(String key:gopTypeWeightMap.keySet()){	
					   	    //	double avgVmTypeUtilizationRate = vmTypeTotalUtilizationMap.get(key)/vmTypeNumberMap.get(key);
					   	    	double minVmTypeUtilizationRate = 1.0;
					   	        long vmToBeCreated = 0;
				                double val = 0.0;
				                double k;
				                
					   	    	
					   	    	if(vmTypeMinUtizationMap.containsKey(key)){
					   	    	     minVmTypeUtilizationRate = vmTypeMinUtizationMap.get(key);
					   	    	}
								
								//If the deadline miss rate of next period of time is higher the upth, and the deadline miss rate
								//compared to current on is increasing, allocate VMs
			                    /*if(gopTypeWeightMap.get(key) >= 0.1 && minVmTypeUtilizationRate> 0.5){ 
			                    	                   	
			                    	if(arrivalRate < 1){
			                    		k =0.5;
			                    	}else{
			                    		k = 0.8;
			                    	}
			                    	
			                    	
			                    	System.out.println("\nThe arrival rate is: " + k);
			                    	
			                        val =k*gopTypeWeightMap.get(key)/DEADLINE_MISS_RATE_UPTH;      			    
								    vmToBeCreated = Math.round(val);
								    provisionVM(vmToBeCreated,key);
			                    }*/
					   	    	
					   	    	
							    if(gopTypeWeightMap.get(key) >= 0.5 && minVmTypeUtilizationRate> 0.5){
							    	
							    	provisionVM(2,key);
									
							    }else if(gopTypeWeightMap.get(key) < 0.5 && gopTypeWeightMap.get(key) > 0.1 && minVmTypeUtilizationRate> 0.5){
							    	provisionVM(1,key);
							    }
					   	    }
						}else{
							System.out.println("\n******The account has hit the maximum vm that can created*****");
						}					
						
					}else if(allcoationDeallocationFlag == 2){//Deallocate Vm
						int vmIndex = 0;	
						
						if(datacenter.getVmList().size() > 1 && runningVmNum >1){
							
									//Looking for the smallest remaining time and utilization VM to be destroyed.
									
									
									double minRemainingTime = 0;
									double minUtilizationRate = 0.0;
								//	double cpuUtilizationRate = 0.0;
									double minCost = 0.0;
									double vmCost = 0.0;
									boolean firstFlag = true;
		
									
									/*List<TranscodingVm> minRemainingTimeVmList = new ArrayList<TranscodingVm>();
									
									for(Vm vm : datacenter.getVmList()){
										TranscodingVm tvm = (TranscodingVm) vm;
										
										if(tvm.getRemainingTime() <= rentingTime/2){
											minRemainingTimeVmList.add(tvm);
										}
									
									}*/
									
									
									//System.out.println("<-----------------------Utilization------------------------------>");
									for(Vm vm : datacenter.getVmList()){
										TranscodingVm vmm = (TranscodingVm) vm;
										VideoSchedulerSpaceShared vmcsch = (VideoSchedulerSpaceShared) vmm.getCloudletScheduler();
		
										/*System.out.println(CloudSim.clock() + "VM#: " + vmm.getId() + "'s renting time until " + vmm.getRentingTime() + 
												 "...The remaining time is: " + vmm.getRemainingTime());*/	
										
									//	System.out.println("The utilization for " + vmm.getVmType() + " is " + cpuUtilizationMap.get(vmm));
										
										if(!cpuUtilizationMap.containsKey(vmm)){
											cpuUtilizationRate = 0.0;
										}else{
											cpuUtilizationRate = cpuUtilizationMap.get(vmm)/(periodicDelay/utilizationCheckPeriod);					
										}
										
										//System.out.println(CloudSim.clock() + "***************The total CPU utilization rate of VM" + vmm.getId() + "(" + vmm.getVmType() + ")" + " is: " + cpuUtilizationRate + " ***************");
										
										//cpuUtilizationMap.put(vmm, 0.0);
										
										InstanceType it = new InstanceType(vmm.getVmType());
                                        vmCost = it.getInstanceCost();
										
										
										if(vmm.getId() != 0  && !vmm.getDeallocationFlag()){
											if(firstFlag || cpuUtilizationRate < minUtilizationRate){
												
												
												minUtilizationRate = cpuUtilizationRate;
												minRemainingTime = vmm.getRemainingTime();	
												minCost = vmCost;	
												vmIndex = vmm.getId();
												firstFlag = false;
												
											
											}else if(minUtilizationRate == cpuUtilizationRate && vmCost < minCost){
												
												minUtilizationRate = cpuUtilizationRate;
												minRemainingTime = vmm.getRemainingTime();
												minCost = vmCost;
												vmIndex = vmm.getId();
												
											}else if(minUtilizationRate == cpuUtilizationRate && vmm.getRemainingTime() < minRemainingTime){
												minUtilizationRate = cpuUtilizationRate;
												minRemainingTime = vmm.getRemainingTime();
												minCost = vmCost;
												vmIndex = vmm.getId();
											}
										}
		
										/*if(minRemainingTimeVmList.size() != 0){
											if(minUtilizationRate == 0.0 || cpuUtilizationRate < minUtilizationRate){
		
												minUtilizationRate = cpuUtilizationRate;
												vmIndex = vmm.getId();
											
											}
											
										}else if(vmm.getRemainingTime() > 0){
											if(minRemainingTime == 0 || vmm.getRemainingTime() < minRemainingTime){
												minRemainingTime = vmm.getRemainingTime();
												vmIndex = vmm.getId();
											
											}
										}else if(vmm.getRemainingTime() <= 0 && vmcsch.getCloudletExecList().size() == 0 && vmcsch.getCloudletWaitingList().size() == 0 && vmcsch.getCloudletPausedList().size() == 0){
									    	//System.out.println(CloudSim.clock() + "\n********************VM_" + vmm.getId() + "'s renting time is over and to be destroyed***********************" );
											sendNow(broker.getVmsToDatacentersMap().get(vmm.getId()), CloudSimTags.VM_DESTROY, vmm);
							                broker.vmDestroyedList.add(vmm);
							                
							                //set Vm's finish time.
							                vmm.setVmFinishTime(CloudSim.clock());
							    	        
							                //Calculate vm cost based on the time it last.
							                broker.setVmCost(vmm);
							                
										}else{
											continue;
										}*/
										
									}	
									
									System.out.println("\n");
									TranscodingVm vmToBeDestroy = (TranscodingVm) broker.getVmList().get(vmIndex);
									
									
									//Check heterogeneous diversity rate
									Transformation tf = new Transformation(broker,datacenter);
									double heterogeneousDiversityRate = tf.getHeterogeneouRate();
									
									
								//	System.out.println(CloudSim.clock() + "VM#: " + vmToBeDestroy.getId() + " is abou to be destroy...\n");
									//Set this vm toBedestroyed flag as true, so that it won't send new cloudlets to this vm again in the future.
									if(vmIndex == 0){
										System.out.println("\n******The account has hit the minimul vm that can have*****\n");
									}else if(minUtilizationRate > 0.5 && heterogeneousDiversityRate > 0.4){
										System.out.println("\n**********The heterogeneous diversity rate is: " + heterogeneousDiversityRate);
									    System.out.println("**********Can't deallocate VM" + vmIndex + " Because its utilization is too high****\n");
									
									}else if(!vmToBeDestroy.getDeallocationFlag()) {
										    System.out.println("\n**********The heterogeneous diversity rate is: " + heterogeneousDiversityRate);
											vmToBeDestroy.setDeallocationFlag(true);
											System.out.println("********* VM" + vmToBeDestroy.getId() + " has been set to be destroyed from deallcation policy********\n" );
					
									}
									
							
								}else{
									System.out.println("\n******The account has hit the minimul vm that can have*****\n");
								}
					   }
					
				       
					
				} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
				}
				
				for(Vm vm : datacenter.getVmList()){
					TranscodingVm vmm = (TranscodingVm) vm;
					cpuUtilizationMap.put(vmm, 0.0);
					
		        }
				
				if(CloudSim.clock() == 1300000){
					System.out.println("test");
				}
				
				if (broker.getCloudletList().size() == 0 && broker.getCloudletNewList().size() == 0 && broker.cloudletSubmittedCount == 0 && jobCount == periodEventNum) {
				    return;
				}else{
					for(Vm vm : datacenter.getVmList()){
						TranscodingVm vmm = (TranscodingVm) vm;
						cpuUtilizationMap.put(vmm, 0.0);
					}

				    send(getId(),periodicDelay,PERIODIC_UPDATE);
	
			    }
			}else{
				/**
				 * fixed vm policy
				 * 
				 */
				
				double cpuUtilizationRate = 0.0;
				for(Vm vm:broker.getVmList()){
					TranscodingVm tvm = (TranscodingVm)vm;	
					
					cpuUtilizationRate = cpuUtilizationMap.get(tvm)/(periodicDelay/utilizationCheckPeriod);
					
					System.out.println(CloudSim.clock() + "***************The total CPU utilization rate of " + tvm.getVmType() + " is: " + cpuUtilizationRate + " ***************");
					cpuUtilizationMap.put(tvm, 0.0);

				}
				
				
				if (broker.getCloudletList().size() == 0 && broker.getCloudletNewList().size() == 0 && broker.cloudletSubmittedCount == 0 && jobCount == periodEventNum) {
				    return;
				}else{
				    send(getId(),periodicDelay,PERIODIC_UPDATE);
	
			    }
				
				//return;
			}
		    
		}
		
		/*public void processProvisioning(){
			*//**
			 * Dynamic Vm policy
			 * 
			 *//*
			
			
			if(vmNum == 0 && !prediction){
				*//**
				 * no prediction mode
				 *//*
	 
				System.out.println(CloudSim.clock() + ": " + "Upating Transcoding provisioning...");
				
				//check vm remaining time;
				for(Vm vm_rt : datacenter.getVmList()){
					TranscodingVm vmm_rt = (TranscodingVm) vm_rt;
					if(vmm_rt.getRemainingTime() <= 0){
					//	System.out.println(CloudSim.clock() + "Vm#: " +  vmm_rt.getId() +" has beyoned renting time, is about to deallocate...");
						vmm_rt.setDeallocationFlag(true);
					}
				}
					
					
				TranscodingProvisioner tp;
				try {
					tp = new TranscodingProvisioner(broker, propertiesFileURL);
					int allcoationDeallocationFlag;
					allcoationDeallocationFlag = tp.allocateDeallocateVm();
					
					System.out.println("The VMList size is: " + datacenter.getVmList().size());
					System.out.println("The Created VMList size is: " + broker.getVmsCreatedList().size());
	
					//1 means allocate new a vm, 2 means deallocate a vm
					if(allcoationDeallocationFlag == 1 ){
						if(datacenter.getVmList().size() < MAX_VM_NUM) {
							//Create a new vm
							List<TranscodingVm> vmNew = (List<TranscodingVm>) createVM(broker.getId(), "c4.large", 1, vmIdShift, rentingTime);
			                vmIdShift++;
			                
			                //submit it to broker
							broker.submitVmList(vmNew);
							
							//creat a event for datacenter to create a vm
							sendNow(datacenter.getId(),CloudSimTags.VM_CREATE_ACK, vmNew.get(0));
						}else{
							System.out.println("\n******The account has hit the maximum vm that can created*****");
						}
						
					}else if(allcoationDeallocationFlag == 3){
						if(datacenter.getVmList().size() < MAX_VM_NUM) {
							//Create two new vms
							System.out.println("creating two vms...");
							List<TranscodingVm> vmNew1 = (List<TranscodingVm>) createVM(broker.getId(), "c4.large", 1, vmIdShift, rentingTime);
			                vmIdShift++;
			                
			                //submit it to broker
							broker.submitVmList(vmNew1);
							
							//creat a event for datacenter to create a vm
							sendNow(datacenter.getId(),CloudSimTags.VM_CREATE_ACK, vmNew1.get(0));
							
							List<TranscodingVm> vmNew2 = (List<TranscodingVm>) createVM(broker.getId(), "c4.large", 1, vmIdShift, rentingTime);
			                vmIdShift++;
			                
			                //submit it to broker
							broker.submitVmList(vmNew2);
							
							//creat a event for datacenter to create a vm
							sendNow(datacenter.getId(),CloudSimTags.VM_CREATE_ACK, vmNew2.get(0));
							
							
							
						}else{
							System.out.println("\n******The account has hit the maximum vm that can created*****");
						}
						
					}else if(allcoationDeallocationFlag == 2){
						int vmIndex = 0;
						int runningVmNum = 0;
						for(Vm vm : datacenter.getVmList()){
							TranscodingVm vmm = (TranscodingVm) vm;
							
							if(vmm.getRemainingTime() >= 0){
								runningVmNum ++;
							}
						}
						
						
						if(datacenter.getVmList().size() > 1 && runningVmNum >1){
							
							//Looking for the smallest completion time VM to be destroyed. 
							Map.Entry<Integer, Double> minCompletionTime_vm = null;
							for (Map.Entry<Integer, Double> entry : broker.totalCompletionTime_vmMap.entrySet()) {
							    if (minCompletionTime_vm == null || minCompletionTime_vm.getValue() > entry.getValue()) {
							        minCompletionTime_vm = entry;
							    }
							}
							if(minCompletionTime_vm == null){
								vmIndex = 0;
							}else {
						        vmIndex = minCompletionTime_vm.getKey();
							}
							
							//find the minimum remaining time vm to deallocate
							
							
							double minRemainingTime = 0;
							
							
							
							for(Vm vm : datacenter.getVmList()){
								TranscodingVm vmm = (TranscodingVm) vm;
								VideoSchedulerSpaceShared vmcsch = (VideoSchedulerSpaceShared) vmm.getCloudletScheduler();

								System.out.println(CloudSim.clock() + "VM#: " + vmm.getId() + "'s renting time until " + vmm.getRentingTime() + 
										 "...The remaining time is: " + vmm.getRemainingTime());								
								if(vmm.getRemainingTime() > 0){
									if(minRemainingTime == 0 || vmm.getRemainingTime() < minRemainingTime){
										minRemainingTime = vmm.getRemainingTime();
										vmIndex = vmm.getId();
									
									}
								}else if(vmm.getRemainingTime() <= 0 && vmcsch.getCloudletExecList().size() == 0 && vmcsch.getCloudletWaitingList().size() == 0 && vmcsch.getCloudletPausedList().size() == 0){
							    	//System.out.println(CloudSim.clock() + "\n********************VM_" + vmm.getId() + "'s renting time is over and to be destroyed***********************" );
									sendNow(broker.getVmsToDatacentersMap().get(vmm.getId()), CloudSimTags.VM_DESTROY, vmm);
					                broker.vmDestroyedList.add(vmm);
					                
					                //set Vm's finish time.
					                vmm.setVmFinishTime(CloudSim.clock());
					    	      
					                //Calculate vm cost based on the time it last.
					                broker.setVmCost(vmm);
					                
								}else{
									continue;
								}
								
							}	
							
							System.out.println("\n");
							TranscodingVm vmToBeDestroy = (TranscodingVm) broker.getVmList().get(vmIndex);
						//	System.out.println(CloudSim.clock() + "VM#: " + vmToBeDestroy.getId() + " is abou to be destroy...\n");
							//Set this vm toBedestroyed flag as true, so that it won't send new cloudlets to this vm again in the future.
							if(!vmToBeDestroy.getDeallocationFlag()) {
								
								vmToBeDestroy.setDeallocationFlag(true);
		
							}
							
						}else{
							System.out.println("\n******The account has hit the minimul vm that can have*****");
						}
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if (broker.getCloudletList().size() == 0 && broker.getCloudletNewList().size() == 0 && broker.cloudletSubmittedCount == 0 && jobCount == periodEventNum) {
				    return;
				}else{
				    send(getId(),periodicDelay,PERIODIC_UPDATE);
	
			    }
			}else if(vmNum == 0 && prediction){
				*//**
				 * prediction mode
				 *//*
				
                System.out.println(CloudSim.clock() + ": " + "Upating Transcoding provisioning...");
				
				//check vm remaining time;
				for(Vm vm_rt : datacenter.getVmList()){
					TranscodingVm vmm_rt = (TranscodingVm) vm_rt;
					if(vmm_rt.getRemainingTime() <= 0){
					//	System.out.println(CloudSim.clock() + "Vm#: " +  vmm_rt.getId() +" has beyoned renting time, is about to deallocate...");
						vmm_rt.setDeallocationFlag(true);
					}
				}
				
                long vmToBeCreated = 0;
                double val = 0.0;
                double futureDmr = 0.0;
                double currentDmr = 0.0;
                double dmrVariation = 0.0;
                double k;
                double arrivalRate = 0.0;
                
                
                
                TranscodingProvisioner tp;
                try{
                	
                	tp = new TranscodingProvisioner(broker, propertiesFileURL);
                	
					currentDmr = tp.getDeadlineMissRate();                	
					futureDmr = tp.getEstimatedDeadlineMissRate() ;
					dmrVariation = futureDmr - currentDmr;
					
					arrivalRate = (jobCount - previousJobCount)*1000.0/periodicDelay;
                	previousJobCount = jobCount;
					
					//If the deadline miss rate of next period of time is higher the upth, and the deadline miss rate
					//compared to current on is increasing, allocate VMs
                    if(dmrVariation >= 0.2 || futureDmr > DEADLINE_MISS_RATE_UPTH){ 
                    	                   	
                    	if(arrivalRate < 1){
                    		k =0.5;
                    	}else{
                    		k = 1;
                    	}
                    	
                    	
                    	System.out.println("\nThe arrival rate is: " + k);
                    	
                        val =k*futureDmr/DEADLINE_MISS_RATE_UPTH;      			    
					    vmToBeCreated = Math.round(val);
					    provisionVM(vmToBeCreated, null);
					    
				    //If the deadline miss rate of next period of time is lower the lowth, and the deadline miss rate
					//compared to current on is decreasing, allocate VMs    
                    }else if(dmrVariation <= -0.2 || futureDmr < DEADLINE_MISS_RATE_LOWTH){ //Deallocate VMs
                    	int vmIndex = 0;
						int runningVmNum = 0;
						for(Vm vm : datacenter.getVmList()){
							TranscodingVm vmm = (TranscodingVm) vm;
							
							if(vmm.getRemainingTime() >= 0){
								runningVmNum ++;
							}
						}
						
						
						if(datacenter.getVmList().size() > 1 && runningVmNum >1){
							
							
							//find the minimum remaining time vm to deallocate
							
							
							double minRemainingTime = 0;
							
							
							
							for(Vm vm : datacenter.getVmList()){
								TranscodingVm vmm = (TranscodingVm) vm;
								VideoSchedulerSpaceShared vmcsch = (VideoSchedulerSpaceShared) vmm.getCloudletScheduler();

								System.out.println(CloudSim.clock() + "VM#: " + vmm.getId() + "'s renting time until " + vmm.getRentingTime() + 
										 "...The remaining time is: " + vmm.getRemainingTime());							
								if(vmm.getRemainingTime() > 0){
									if(minRemainingTime == 0 || vmm.getRemainingTime() < minRemainingTime){
										minRemainingTime = vmm.getRemainingTime();
										vmIndex = vmm.getId();
									
									}
								}else if(vmm.getRemainingTime() <= 0 && vmcsch.getCloudletExecList().size() == 0 && vmcsch.getCloudletWaitingList().size() == 0 && vmcsch.getCloudletPausedList().size() == 0){
							    	System.out.println(CloudSim.clock() + "\n********************VM_" + vmm.getId() + "'s renting time is over and to be destroyed***********************" );
									sendNow(broker.getVmsToDatacentersMap().get(vmm.getId()), CloudSimTags.VM_DESTROY, vmm);
					                broker.vmDestroyedList.add(vmm);
					                
					                //set Vm's finish time.
					                vmm.setVmFinishTime(CloudSim.clock());
					    	      
					                //Calculate vm cost based on the time it last.
					                broker.setVmCost(vmm);
					                
								}else{
									continue;
								}
								
							}	
							
							System.out.println("\n");
							TranscodingVm vmToBeDestroy = (TranscodingVm) broker.getVmList().get(vmIndex);
						//	System.out.println(CloudSim.clock() + "VM#: " + vmToBeDestroy.getId() + " is abou to be destroy...\n");
							//Set this vm toBedestroyed flag as true, so that it won't send new cloudlets to this vm again in the future.
							if(!vmToBeDestroy.getDeallocationFlag()) {
								
								vmToBeDestroy.setDeallocationFlag(true);
		
							}else{
								System.out.println("\n******The account has hit the minimul vm that can have*****");
							}
						}
                    	
                    }
                    
                    if (broker.getCloudletList().size() == 0 && broker.getCloudletNewList().size() == 0 && broker.cloudletSubmittedCount == 0 && jobCount == periodEventNum) {
    				    return;
    				}else{
    				    send(getId(),periodicDelay,PERIODIC_UPDATE);
    	
    			    }
    			    
    			    
                }catch(IOException e){
                
					// TODO Auto-generated catch block
					e.printStackTrace();
    				
                }
                
               
				
			}else{
				*//**
				 * fixed vm policy
				 * 
				 *//*
				
				double cpuUtilizationRate = 0.0;
				for(Vm vm:broker.getVmList()){
					TranscodingVm tvm = (TranscodingVm)vm;	
					
					cpuUtilizationRate = cpuUtilizationMap.get(tvm)/(periodicDelay/utilizationCheckPeriod);
					
					System.out.println(CloudSim.clock() + "***************The total CPU utilization rate of " + tvm.getVmType() + " is: " + cpuUtilizationRate + " ***************");
					cpuUtilizationMap.put(tvm, 0.0);

				}
				
				
				if (broker.getCloudletList().size() == 0 && broker.getCloudletNewList().size() == 0 && broker.cloudletSubmittedCount == 0 && jobCount == periodEventNum) {
				    return;
				}else{
				    send(getId(),periodicDelay,PERIODIC_UPDATE);
	
			    }
				
				//return;
			}
		    
		}*/
		
		
		
		/**
		 * Allocate VMs based on Startup Queue Length
		 */
		public void provisionVM(long vmNum, String vmType){
			
			if(vmType == null){
				vmType = "c4.xlarge";
			}
			
			//	System.out.println("\n**creating "+ vmNum + " " + vmType + " vms...\n");

			for(int i=0; i<vmNum; i++){
			    List<TranscodingVm> vmNew = (List<TranscodingVm>) createVM(broker.getId(), vmType, 1, vmIdShift, rentingTime);
                vmIdShift++;
	            
	            //submit it to broker
				broker.submitVmList(vmNew);
				
				//creat a event for datacenter to create a vm
		    	sendNow(datacenter.getId(),CloudSimTags.VM_CREATE_ACK, vmNew.get(0));
			
		    }
			
		}
		
		
		/**
		 * Create a periodic event for process new jobs and update resource provisioning
		 * periodEventNumb is the number of periodic events
		 * @throws IOException 
		 * 
		 */
		
		public void periodicUpdate(){
					
			/*if (broker.generatePeriodicEvent)  {
				//processNewJobs();
				int jobDelay = 0;
				for(int i=0; i<periodEventNum; i++){
					Random random = new Random();
				    jobDelay += (int)getRandomNumber(1000, 5000, random);	
					send(getId(),jobDelay,CREATE_JOB);
					
				}
				//periodCount++;
		    	broker.generatePeriodicEvent = false;

				
		    }
			//Set the the number of periodic event to create new job, so that it won't run forever.
		    if (periodCount < periodEventNum) {
		    	  broker.generatePeriodicEvent = true;
		    	  
		    }else{
		    	  broker.generatePeriodicEvent = false;
		    }
		      
		    */
          
		   //when cloudlets are finished, stop periodic event
		  //  if (broker.getCloudletList().size() == 0 && broker.getCloudletNewList().size() == 0 && broker.cloudletSubmittedCount == 0) {
		    /*if (broker.getCloudletList().size() == 0 && broker.getCloudletNewList().size() == 0 && broker.cloudletSubmittedCount == 0 ) {
		    	return;
		    }
		    
			processProvisioning();*/
		  //  send(getId(),periodicDelay,PERIODIC_UPDATE);
		}
        
		
		public List<TranscodingVm> getVmList() {
			return vmList;
		}

		protected void setVmList(List<TranscodingVm> vmList) {
			this.vmList = vmList;
		}

		public List<Binary> getCloudletList() {
			return cloudletList;
		}

		protected void setCloudletList(List<Binary> cloudletList) {
			this.cloudletList = cloudletList;
		}

		public TranscodingBroker getBroker() {
			return broker;
		}

		public void setBroker(TranscodingBroker broker) {
			this.broker = broker;
		}
		
		public int  getJobCount(){
			return jobCount;
		}
		
		public double  getJobDelay(){
			return jobDelay;
		}
		
		

	}
}
