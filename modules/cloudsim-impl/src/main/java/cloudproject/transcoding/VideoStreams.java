package cloudproject.transcoding;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.core.CloudSim;

public class VideoStreams implements Callable<String>{
	
	private String command;
	private static volatile boolean volatile_flag = false;
	
	//New arrival queue size
	private final int NEW_ARRIVAL_QUEUE_SIZE = 4;
	
    //broker Id, every instance has its own userId
	private int userId;
	
	//video Id
	private int videoId;
	//The number of cloudlets are gonna be created, every instance has its own cloudlet number
	private int cloudlets;
	//Every instance has its own cloudletList
	private List<VideoSegment> cloudletList;
	//All the instance share cloudletNewArrivalQueue and cloudletBatchqueue, both of them are synchronized list
	private static List<VideoSegment> cloudletNewArrivalQueue = Collections.synchronizedList(new ArrayList<VideoSegment>());
	private static List<VideoSegment> cloudletBatchQueue = Collections.synchronizedList(new ArrayList<VideoSegment>());
	private static List<VideoSegment> cloudletNewList = new ArrayList<VideoSegment>();

	//private static ArrayList<Integer> newcloudlets = new ArrayList<Integer> ();

    private static int testCount = 0;
    private static int fileCount = 0;
    
	private String inputdataFolderURL;
	public boolean startupqueue;
	public int seedShift = 0;
	public String estimatedGopLength = null;
	private static int videoCount = 0;

    
	
	public VideoStreams(){
		
	}
	
	public VideoStreams(String s, String inputdataFolderURL, boolean startupqueue, String estimatedGopLength, int seedShift, int userId, int videoId, int cloudlets){
	    this.command = s;
	    this.inputdataFolderURL = inputdataFolderURL;
	    this.startupqueue = startupqueue;
	    this.userId = userId;
	    this.videoId = videoId;
	    this.cloudlets = cloudlets;
	    this.seedShift = seedShift;
	    this.estimatedGopLength = estimatedGopLength;
	}

    public String call() {
    	// do stuff and return some String
         //currentTime = System.nanoTime()/(double)1000000;
    	
         //System.out.println(Thread.currentThread().getName()+" Start. Video Stream_"+ command);

         processCommand();
        // System.out.println(Thread.currentThread().getName()+" End.");         
            
         return Thread.currentThread().getName();
     }
    
     public boolean accept(File file) {
        return !file.isHidden();
     }
    
     private void processCommand() {
    	 //Read the files in the datafile folder	
		//File folder = new File("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudsim-impl/resources/inputdatafile"); 
		File folder = new File(inputdataFolderURL);

		File[] listOfFiles = folder.listFiles();
		
		/*Random randomSeed = new Random(seedShift);
		int shift = randomSeed.nextInt(1000)+1000;*/
		
		Random random = new Random(fileCount + seedShift*100);
		
		int fileIndex = random.nextInt(1000)%listOfFiles.length;
		
		if(fileIndex < listOfFiles.length){
			File file = listOfFiles[fileIndex];
			//if the file is hidden file, it won't be accepted and move to next one
			while(!accept(file)){
			  fileCount++;
			  fileIndex++;
			  file = listOfFiles[fileIndex];
			  
			}
			
		     
		    if (file.isFile() && file.getName().endsWith(".txt")) {
			   fileCount++;
		       cloudletList = createCloudlet(userId, videoId, file);
			   
		    }
		    
		   
			// cloudletList.clear();
			 System.out.println("\n**************************Video Stream_"+ command + "  " + file.getName() + " just arrived**************************\n"); 

			 System.out.println(Thread.currentThread().getName() +" Video Stream_"+ command + ": Created " + cloudletList.size() + " Cloudlets\n"); 
				// cloudletBatchQueue.addAll(cloudletList);
			 ArrayList<Integer> newcloudlets = new ArrayList<Integer> ();
         
		 
			 synchronized (this) {
				     if(startupqueue){
				    	 /**
		    		      * with new arrival queue
		    		      *   
		    		      */
		    		     if(cloudletList.size() > NEW_ARRIVAL_QUEUE_SIZE){
				    		 cloudletNewArrivalQueue.clear();

		    		    	 for(int i=0; i < NEW_ARRIVAL_QUEUE_SIZE; i++) {
				    			 cloudletNewArrivalQueue.add(cloudletList.get(i));
				    		 }
				    		 //The rest of the cloudlet list are sent to batch queue
				    		 for(int j=NEW_ARRIVAL_QUEUE_SIZE; j<cloudletList.size(); j++ ){
				    			 cloudletBatchQueue.add(cloudletList.get(j));
				    		 }
		    		     }else{
		    		    	 cloudletNewArrivalQueue.clear();
		    		    	 for(int i=0; i < cloudletList.size(); i++) {
				    			 cloudletNewArrivalQueue.add(cloudletList.get(i));
				    			
				    		 }
		    		    	 
		    		     }
				     }else{
				    	 /**
		    		      * without new arrival queue
		    		      *   
		    		      */
		    		  
		        		 for(int j=0; j<cloudletList.size(); j++ ){
			    			 cloudletBatchQueue.add(cloudletList.get(j));
			    		 }

				    	 
				    	 
				     }

				    
			    	 System.out.println(Thread.currentThread().getName() + "*****New arrival queue Video ID_" + videoId + ": " + cloudletNewList + " **********");
				// }
			   }
			 
		 
		     /*System.out.println(Thread.currentThread().getName() +" Video Stream_"+ command + 
		    		 " Thread is going to sleep " + sleepTime + "ms."); 
		     Thread.sleep(sleepTime);*/
		 }
     }
     
     private List<VideoSegment> createCloudlet(int userId, int videoId, File file){
 		// Creates a container to store Cloudlets
 		LinkedList<VideoSegment> list = new LinkedList<VideoSegment>();

        /*ParseData pd = new ParseData();
        pd.parseData(file);*/
 		
 		CreateGopData gopData = new CreateGopData(); 
        
        //combine 10 times results to one, and then combine different instance's resulte to one.
 		gopData.ParseCloudlet(file);
	
        
      //cloudlet parameters
 		long length = 0;
 	    Map<String, Long> cloudletLengthMap = new HashMap<String, Long>();
 		double deadline;
 		long fileSize = 0;
 		long outputSize = 0;
 		int cloudletNum = gopData.getGopIdList().size(); 
 		//int cloudletNum = cloudlets;
 		int pesNumber = 1;
 		
 		
 		UtilizationModel utilizationModel = new UtilizationModelFull();

 		VideoSegment[] cloudlet = new VideoSegment[cloudletNum];
 		

 		for(int i=0;i<cloudletNum;i++){
 		
 			 		    
 		    cloudletLengthMap.putAll(gopData.getGopengthMapList().get(i));
 			deadline = gopData.getGopPtsList().get(i);
 			fileSize = gopData.getGopInputSizeList().get(i);
 			outputSize = gopData.getGopOutputSizeList().get(i);
 			
 			
 			double arrivalTime = CloudSim.clock();//FIX HERE
			double deadlineBeforePlay = Double.MAX_VALUE - cloudletNum + i;
			double deadlineAfterPlay = deadline;
			
			double alpha = 0.01;
			double beta = 1 / Math.E;
			double utilityNum;
			
		    utilityNum = Math.pow(beta, alpha * i) + 1.0/(videoId+1000);

			/*if(i==0){
			    utilityNum = Math.pow(beta, alpha * i) + 1.0/(videoId+1);
			}else{
				utilityNum = Math.pow(beta, alpha * i);
			}*/
			int orderNum = i;
			
			
			//Define GOP Type
			
			Map<String, Double> gopTypeMap = new HashMap<String, Double>();
			double weight = 0.0;
			double lowestWeight = Double.MAX_VALUE;
			double sigma = 0.4;
			String gopType = null;
			
			
			
						
			double min_t = Double.MAX_VALUE;
			double min_c = Double.MAX_VALUE;
			double max_t = 0.0;
			double max_c = 0.0;
			
			
			
            for(String vmType: cloudletLengthMap.keySet()){

				InstanceType it = new InstanceType(vmType);			
				double t = cloudletLengthMap.get(vmType)/(1000.0*it.getInstanceMips());
			    double c = it.getInstanceCost();	
			    double gop_c = t*c;
			    
			    if(t < min_t){
			    	min_t = t;
			    }
			    
			    if(t > max_t){
			    	max_t = t;
			    }
			    
			    if(gop_c < min_c){
			    	min_c = gop_c;
			    }
			    
			    if(gop_c > max_c){
			    	max_c = gop_c;
			    }
			    
			    
			}
            
            /*
            
            if(CloudSim.clock() > 447161){
            	System.out.println("test");
            }*/
			
			for(String vmType: cloudletLengthMap.keySet()){
				
				InstanceType it = new InstanceType(vmType);
				
				double t = cloudletLengthMap.get(vmType)/(1000.0*it.getInstanceMips());
				double c = it.getInstanceCost();
				double gop_c = t*c;
				
				double t_rate = (t - min_t)/(max_t - min_t);
				double c_rate = (gop_c - min_c)/(max_c - min_c);
				
				weight = sigma*t_rate + (1-sigma)*c_rate;
				//weight = c_rate;
				//weight = t_rate;
                
			    //System.out.println("******" + vmType + " to GPU is: " + new DecimalFormat("#0.0000").format(weight));
				
				gopTypeMap.put(vmType, weight);
				
				if(lowestWeight > weight){
					 
					lowestWeight = weight;
					gopType = vmType;	
				
				}		
				
			}
			
			System.out.println("*****************************************This GOP's type is: " + gopType);

			
			//System.out.println(utilityNum);
			
 		    cloudlet[i] = new VideoSegment(i, utilityNum, orderNum, videoId, length, cloudletLengthMap, arrivalTime, deadlineBeforePlay, deadlineAfterPlay, pesNumber, fileSize, outputSize, gopTypeMap, utilizationModel, utilizationModel, utilizationModel);
 			
 		    
 		    // setting the owner of these Cloudlets		
 		    cloudlet[i].setUserId(userId);
 			list.add(cloudlet[i]);
 			
 		}
        
 		volatile_flag = true;
 		
 		return list;
 		  
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
 	
 	public List<VideoSegment> getBatchQueue() {
 		return cloudletBatchQueue;
 	}
 	public List<VideoSegment> getNewArrivalList(){
 		return cloudletNewList;
 	}
 	
 	
 	public void setBatchQueue(List<VideoSegment> cloudletBatchQueue){
 		this.cloudletBatchQueue = cloudletBatchQueue;
 	}
 	
 	public List<VideoSegment> getNewArrivalQueue(){
 		return cloudletNewArrivalQueue;
 	}
 	
 	public void setNewArrivalQueue(List<VideoSegment> cloudletNewArrivalQueue){
 		this.cloudletNewArrivalQueue = cloudletNewArrivalQueue;
 	}

}

