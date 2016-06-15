package cloudproject.test;

import io.humble.video.MediaPacket;
import io.humble.video.VideoJNI;


public class MediaPacketExtend extends MediaPacket{
	
	protected MediaPacketExtend(long cPtr, boolean cMemoryOwn) {
		super(cPtr, cMemoryOwn);
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * Create a new Packet
	 */
	  public static MediaPacketExtend make() {
	    long cPtr = VideoJNI.MediaPacket_make__SWIG_0();
	    return (cPtr == 0) ? null : new MediaPacketExtend(cPtr, false);
	  }

	public void setComplete(boolean complete) {
		VideoJNI_Extention.MediaPacket_setComplete(getMyCPtr(), this, complete);
	}

}
