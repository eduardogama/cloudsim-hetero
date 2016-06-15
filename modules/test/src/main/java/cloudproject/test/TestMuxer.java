

package cloudproject.test;


import io.humble.video.Decoder;
import io.humble.video.Demuxer;
import io.humble.video.DemuxerFormat;
import io.humble.video.DemuxerStream;
import io.humble.video.KeyValueBag;
import io.humble.video.MediaAudio;
import io.humble.video.MediaPacket;
import io.humble.video.MediaPicture;
import io.humble.video.Muxer;
import io.humble.video.MediaDescriptor.Type;



public class TestMuxer {

  public static void main(String arg[]) throws Exception {
	  
	
  //Print out everything in the console into the file
    printOutputFile pof = new printOutputFile();  
    pof.printOutToFile("BigBuckBunny_320x180_v5"); 

    final String f = "/Users/lxb200709/Documents/TransCloud/videosource/BigBuckBunny_320x180.mp4";
    final String o = "/Users/lxb200709/Documents/TransCloud/videosource/BigBuckBunny_320x180_v4.mp4";
    
    Demuxer demuxer = Demuxer.make();
    demuxer.open(f, null, false, true, null, null);
    
    final DemuxerFormat format = demuxer.getFormat();
    System.out.printf("URL: '%s' (%s: %s)\n", demuxer.getURL(), format.getLongName(), format.getName());

    // Many programs that make containers, such as iMovie or Adobe Elements, will
    // insert meta-data about the container. Here we extract that meta data and print it.
    KeyValueBag metadata = demuxer.getMetaData();
    System.out.println("MetaData:");
    for(String key: metadata.getKeys())
      System.out.printf("  %s: %s\n", key, metadata.getValue(key));
    
    System.out.println("\n");
    
    // There are a few other key pieces of information that are interesting for
    // most containers; The duration, the starting time, and the estimated bit-rate.
    // This code extracts all three.
    final String formattedDuration = formatTimeStamp(demuxer.getDuration());
    System.out.printf("Duration: %s, start: %f, bitrate: %d kb/s\n",
        formattedDuration,
        demuxer.getStartTime() == Global.NO_PTS ? 0 : demuxer.getStartTime() / 1000000.0,
            demuxer.getBitRate()/1000);
    
    System.out.println("\n");
    
    
    // open output
    Muxer muxer = Muxer.make(o, null, null);
    // Finally, a container consists of several different independent streams of
    // data called Streams. In Humble there are two objects that represent streams:
    // DemuxerStream (when you are reading) and MuxerStreams (when you are writing).
    
    // First find the number of streams in this container.
    int ns = demuxer.getNumStreams();
    
    final Decoder[] decoders = new Decoder[ns];
    
    MediaPicture picture = null;
    MediaAudio samples = null;

    
    // Now, let's iterate through each of them.
    for (int i = 0; i < ns; i++) {
      
      DemuxerStream stream = demuxer.getStream(i);

      metadata = stream.getMetaData();
      // Language is usually embedded as metadata in a stream.
      final String language = metadata.getValue("language");
      
      // We will only be able to make a decoder for streams we can actually
      // decode, so the caller should check for null.
      decoders[i] = stream.getDecoder();


      System.out.printf(" Stream #0.%1$d (%2$s): %3$s\n", i, language, decoders[i] != null ? decoders[i].toString() : "unknown coder");
      System.out.println("  Metadata:");
      for(String key: metadata.getKeys())
        System.out.printf("    %s: %s\n", key, metadata.getValue(key));
     
      if(decoders[i].getCodecType() == Type.MEDIA_VIDEO) {
          System.out.printf("    frame rate: %s\n", stream.getFrameRate());
          System.out.printf("    frame number: %s\n", stream.getNumFrames());
          System.out.printf("    stream tb: %s\n", stream.getTimeBase()); 
          
          //Open the video decoder
          decoders[i].open(null, null);
      }
      
      if(decoders[i].getCodecType() == Type.MEDIA_AUDIO) {
    	  decoders[i].open(null, null);
    	  
      }
      System.out.println("\n");

      muxer.addNewStream(decoders[i]);

    }
    
    muxer.open(null, null);
    
    MediaPacket ipacket = MediaPacket.make();
    while(demuxer.read(ipacket) >= 0) {
      
      	
      muxer.write(ipacket, false);
    }
    muxer.close();
    
    // now, let's read in the file we wrote and confirm correct # of packets and size.
    Demuxer demuxer2 = Demuxer.make();
    demuxer2.open(o,  null, false, true, null, null);
    int numPackets = 0;
    while(demuxer2.read(ipacket) >= 0)
      ++numPackets;
    
    demuxer.close();
    demuxer2.close();
  }
  
  
  /**
   * Pretty prints a timestamp (in {@link Global.NO_PTS} units) into a string.
   * @param duration A timestamp in {@link Global.NO_PTS} units).
   * @return A string representing the duration.
   */
  private static String formatTimeStamp(long duration) {
    if (duration == Global.NO_PTS) {
      return "00:00:00.00";
    }
   // System.out.print("Globle: " + Global.DEFAULT_PTS_PER_SECOND);
    double d = 1.0 * duration / Global.DEFAULT_PTS_PER_SECOND;
    int hours = (int) (d / (60*60));
    int mins = (int) ((d - hours*60*60) / 60);
    int secs = (int) (d - hours*60*60 - mins*60);
    int subsecs = (int)((d - (hours*60*60.0 + mins*60.0 + secs))*100.0);
    return String.format("%1$02d:%2$02d:%3$02d.%4$02d", hours, mins, secs, subsecs);
  }	
  
  private static String formatStreamTimeStamp(IStream stream, long duration) {
	    if (duration == Global.NO_PTS) {
	      return "00:00:00.00";
	    }
	   // System.out.print("Globle: " + Global.DEFAULT_PTS_PER_SECOND);
	    double d = 1.0 * duration / stream.getTimeBase().getDenominator();
	    int hours = (int) (d / (60*60));
	    int mins = (int) ((d - hours*60*60) / 60);
	    int secs = (int) (d - hours*60*60 - mins*60);
	    int subsecs = (int)((d - (hours*60*60.0 + mins*60.0 + secs))*100.0);
	    return String.format("%1$02d:%2$02d:%3$02d.%4$02d", hours, mins, secs, subsecs);
  }	  
  
  private static String formatPacketTimeStamp(IPacket packet, long duration) {
	    if (duration == Global.NO_PTS) {
	      return "00:00:00.00";
	    }
	   // System.out.print("Globle: " + Global.DEFAULT_PTS_PER_SECOND);
	    double d = 1.0 * duration / packet.getTimeBase().getDenominator();
	    int hours = (int) (d / (60*60));
	    int mins = (int) ((d - hours*60*60) / 60);
	    int secs = (int) (d - hours*60*60 - mins*60);
	    int subsecs = (int)((d - (hours*60*60.0 + mins*60.0 + secs))*100.0);
	    return String.format("%1$02d:%2$02d:%3$02d.%4$02d", hours, mins, secs, subsecs);
  }	

}

