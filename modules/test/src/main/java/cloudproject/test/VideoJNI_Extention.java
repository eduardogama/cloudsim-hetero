package cloudproject.test;

import io.humble.video.MediaPacket;
import io.humble.video.VideoJNI;

public class VideoJNI_Extention extends VideoJNI{
	
	public final static native void MediaPacket_setComplete(long jarg1, MediaPacket jarg1_, boolean jarg2);

}
