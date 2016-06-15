package cloudproject.transcoding;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class CreateGopData {
	private String transcodedTo;
	private ArrayList<Integer> gopIdList = new ArrayList<Integer>();
	private ArrayList<Integer> deviationGopTranscodingTimeList = new ArrayList<Integer>();
	private ArrayList<Integer> gopPtsList = new ArrayList<Integer>();
	private ArrayList<Integer> gopInputSizeList = new ArrayList<Integer>();
	private ArrayList<Integer> gopOutputSizeList = new ArrayList<Integer>();	
	private ArrayList<Integer> stdList = new ArrayList<Integer>();	
	private ArrayList<HashMap<String, Long>> gopTranscodingTimeMapList = new ArrayList<HashMap<String, Long>>();
	
	public CreateGopData(){
		
	}
	
	public String getTranscodedTo() {
		return transcodedTo;
	}

	public void setTranscodedTo(String transcodedTo) {
		this.transcodedTo = transcodedTo;
	}
	
	public ArrayList<HashMap<String, Long>> getGopengthMapList() {
		return gopTranscodingTimeMapList;
	}

	public void addToGopLengthMapList(String ec2Type, Long transcodingTime) {
		HashMap<String, Long> tempMap = new HashMap<String, Long>();
		long gopLength;
		
		MapInstanceToGopLength mig= new MapInstanceToGopLength();		
		gopLength = mig.getGopLength(ec2Type, transcodingTime);
		
		tempMap.put(ec2Type, gopLength);
		this.gopTranscodingTimeMapList.add(tempMap);
	}
	
	
	public ArrayList<Integer> getGopIdList() {
		return gopIdList;
	}

	public void addToGopIdList(int gopId) {
		this.gopIdList.add(gopId);
	}

	public ArrayList<Integer> getDeviationGopTranscodingTimeList() {
		return deviationGopTranscodingTimeList;
	}

	public void addToDeviationGopTranscodingTimeList(int deviationGopTranscodingTime) {
		this.deviationGopTranscodingTimeList.add(deviationGopTranscodingTime);
	}

	public ArrayList<Integer> getGopPtsList() {
		return gopPtsList;
	}

	public void addToGopPtsList(int gopPts) {
		this.gopPtsList.add(gopPts);
	}

	public ArrayList<Integer> getGopInputSizeList() {
		return gopInputSizeList;
	}

	public void addToGopInputSizeList(int gopInputSize) {
		this.gopInputSizeList.add(gopInputSize);
	}

	public ArrayList<Integer> getGopOutputSizeList() {
		return gopOutputSizeList;
	}

	public void addToGopOutputSizeList(int gopOutputSize) {
		this.gopOutputSizeList.add(gopOutputSize);
	}

	public ArrayList<Integer> getStdList() {
		return stdList;
	}

	public void addToStdList(int std) {
		this.stdList.add(std);
	}
	
	public void ParseCloudlet(File videoDataFile){
		
        /**
         * Combine different instance's transcoding time into a map <instance type, transcoding time>
         */
        
		//File videoDataFile = new File(cloudletUrl);
		BufferedReader br = null;
	       
		 try {
			
				FileReader fr = new FileReader(videoDataFile);
		        br = new BufferedReader(fr);
		        
		        String sCurrentLine;
		        String splitterFlag = "TranscodingType";
		        boolean flag = true;
		        int index = 1;
		        long gopLength = 0;
		        
		        if (videoDataFile.isFile() && videoDataFile.getName().endsWith(".txt")){
			        while ((sCurrentLine = br.readLine()) != null) {
			            if(sCurrentLine.length() > 0) {
				            String[] arr = sCurrentLine.split("\\s+");			            
				            if(arr[0].equals(splitterFlag)){
				            	if(index == 0){
				            		flag = false;
				            	}
				            	index = 0;
				            	continue;
	 	            		}else {
	 	            			if(flag){		 	            			
	 	            				
		 	            			setTranscodedTo(arr[0]);
		 	            			addToGopLengthMapList(arr[1], Long.parseLong(arr[3]));
		 	            			addToGopIdList(Integer.parseInt(arr[2]));
		 	            			addToGopPtsList(Integer.parseInt(arr[4]));
		 	            			addToGopInputSizeList(Integer.parseInt(arr[5]));
		 	            			addToGopOutputSizeList(Integer.parseInt(arr[6]));

	 	            			}else{
	 	            				MapInstanceToGopLength mitl = new MapInstanceToGopLength();
	 	            				gopLength = mitl.getGopLength(arr[1], Long.parseLong(arr[3]));
	 	            				
		 	            			getGopengthMapList().get(index).put(arr[1], gopLength);
		 	            			index++;
	 	            			}
	 	            		}
			            }
			        }
		        }
		 } catch (IOException e) {
		        e.printStackTrace();
		 } finally {
		       try {
		            if (br != null)br.close();
		        } catch (IOException ex) {
		            ex.printStackTrace();
		        }
		   }     

	}

}
