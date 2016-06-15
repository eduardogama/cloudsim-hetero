package cloudproject.transcoding;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;


public class VideoSegment extends Cloudlet{
	
	private double cloudletDeadline;
	//The time when cloudlet are created and put in the batch queue, which is different with the
	//the arrival time in ResCloudlet which is the time when cloudlet are queued in vm local queue
	private double arrivalTime;
	private double finishTime;
	private double cloudletDeadlineAfterPlay;
	private int videoId;
	private int status;
	private boolean record;
	private int std;
	private long avglength;
	private Map<String, Long> cloudletLengthMap = new HashMap<String, Long>();
	private String segmentExecutedVmType;
	private double utilityNum;
	private int orderNum;
	private Map<String, Double> gopTypeMap;
	//Create a buffer to store the video gop raw data
	//Buffer buffer = Buffer.make(null, 1000);

	

	public VideoSegment(
			final int cloudletId,
			final double utilityNum,
			final int orderNum,
			final int videoId,
		    final long cloudletLength,
		    final Map<String, Long> cloudletLengthMap,
			final double arrivalTime,
		    final double cloudletDeadline,
		    final double cloudletDeadlineAfterPlay,
			final int pesNumber,
			final long cloudletFileSize,
			final long cloudletOutputSize,
			final Map<String, Double> gopTypeMap,
			final UtilizationModel utilizationModelCpu,
			final UtilizationModel utilizationModelRam,
			final UtilizationModel utilizationModelBw) {
		super(
				cloudletId,
				cloudletLength,
				pesNumber,
				cloudletFileSize,
				cloudletOutputSize,
				utilizationModelCpu,
				utilizationModelRam,
				utilizationModelBw,
				false
				);
	   this.utilityNum = utilityNum;
	   this.orderNum = orderNum;
	   this.cloudletLengthMap.putAll(cloudletLengthMap); 
	   this.arrivalTime = arrivalTime;
	   this.cloudletDeadline = cloudletDeadline;
	   this.videoId = videoId;
	   this.cloudletDeadlineAfterPlay = cloudletDeadlineAfterPlay;
	   this.gopTypeMap = gopTypeMap;
	   
		
	}
	
	/**
	 * Get GOP Type
	 * @return
	 */
	
	
	public Map<String, Double> getGopTypeMap() {
		return gopTypeMap;
	}

    /**
     * Set GOP Type
     * @param gopType
     */
	public void setGopType(Map<String, Double> gopType) {
		this.gopTypeMap = gopTypeMap;
	}

	/**
	 * get video segment's utility number
	 * @return
	 */
	public double getUtilityNum() {
		return utilityNum;
	}
	
	/**
	 * set video segment's utility number
	 * @param utilityNum
	 */

	public void setUtilityNum(double utilityNum) {
		this.utilityNum = utilityNum;
	}
	
	/**
	 * get the order number of this segment in each video
	 * @return
	 */
	public int getOrderNum() {
		return orderNum;
	}

	/**
	 * set the order number of the segmetn in each video
	 * @param orderNum
	 */
	public void setOrderNum(int orderNum) {
		this.orderNum = orderNum;
	}
	
	/**
	 * get the vm type that executed this video segment
	 * @return
	 */
	public String getSegmentExecutedVmType() {
		return segmentExecutedVmType;
	}


	public void setSegmentExecutedVmType(String segmentExecutedVmType) {
		this.segmentExecutedVmType = segmentExecutedVmType;
	}
	
	public Map<String, Long> getCloudletLengthMap() {
		return cloudletLengthMap;
	}


	public long getAvgCloudletLength(){
		return avglength;
	}
	
	public long getCloudletStd(){
		return std;
	}
	
	
	public double getArrivalTime(){
		return arrivalTime;
	}
	
	public double getCloudletDeadline() {
		return cloudletDeadline;
	}
	
	public void setCloudletDeadline(double cloudletDeadline){
		this.cloudletDeadline = cloudletDeadline;
	}
	
	public double getCloudletDeadlineAfterPlay(){
		return cloudletDeadlineAfterPlay;
	}
	
	public void setCloudletDeadlineAfterPlay(double cloudletDeadlineAfterPlay){
		this.cloudletDeadlineAfterPlay = cloudletDeadlineAfterPlay;
	}
	
	public int getCloudletVideoId() {
		return videoId;
	}
		
}
