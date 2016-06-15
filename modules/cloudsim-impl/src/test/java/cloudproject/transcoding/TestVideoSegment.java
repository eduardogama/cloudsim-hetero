package cloudproject.transcoding;

import io.humble.ferry.Buffer;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.core.CloudSim;


public class TestVideoSegment extends Cloudlet{
	
	private double cloudletDeadline;
	//The time when cloudlet are created and put in the batch queue, which is different with the
	//the arrival time in ResCloudlet which is the time when cloudlet are queued in vm local queue
	private double arrivalTime;
	private double finishTime;
	private int videoId;
	private int status;
	private boolean record;
	
	//Create a buffer to store the video gop raw data
	Buffer buffer = Buffer.make(null, 1000);

    
	
	public TestVideoSegment(
			final int cloudletId,
			final int videoId,
			final long cloudletLength,
			final double arrivalTime,
		    final double cloudletDeadline,
			final int pesNumber,
			final long cloudletFileSize,
			final long cloudletOutputSize,
			final Buffer buffer,
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
	   this.arrivalTime = arrivalTime;
	   this.cloudletDeadline = cloudletDeadline;
	   this.videoId = videoId;
	   this.buffer = buffer;
		
	}
		
	
	
	public double getArrivalTime(){
		return arrivalTime;
	}
	
	public double getCloudletDeadline() {
		return cloudletDeadline;
	}
	
	public int getCloudletVideoId() {
		return videoId;
	}
	
	public Buffer getData(){
		return buffer;
	}	
	
}





