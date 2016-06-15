
/*******************************************************************************
 * Copyright (c) 2014, Art Clarke.  All rights reserved.
 *  
 * This file is part of Humble-Video.
 *
 * Humble-Video is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Humble-Video is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Humble-Video.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package cloudproject.test;

import java.io.IOException;
import java.util.ArrayList;

import io.humble.ferry.Buffer;
import io.humble.video.BitStreamFilter;
import io.humble.video.Container;
import io.humble.video.Decoder;
import io.humble.video.Demuxer;
import io.humble.video.DemuxerFormat;
import io.humble.video.DemuxerStream;
import io.humble.video.Global;
import io.humble.video.KeyValueBag;
import io.humble.video.MediaAudio;
import io.humble.video.MediaPacket;
import io.humble.video.MediaPicture;
import io.humble.video.Muxer;
import io.humble.video.MuxerFormat;
import io.humble.video.MediaDescriptor.Type;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Demo application that takes a Media file and displays the known meta-data about it.
 * <p>
 * Concepts introduced:
 * </p>
 * <ul>
 * <li>Demuxers: An {@link Demuxer} object can read from Media {@link Container} objects.</li>
 * <li>DemuxerStreams: {@link DemuxerStream} objects represent Streams of media information inside a {@link Container}.</li>
 * <li>KeyValueBags: {@link KeyValueBag} objects are used throughout Humble to represent key-value meta-data stored inside different objects.</li>
 * </ul>
 * <p>
 * To run from maven, do:
 * </p>
 * <pre>
 * mvn install exec:java -Dexec.mainClass="io.humble.video.demos.GetContainerInfo" -Dexec.args="filename.mp4"
 * </pre>
 * @author aclarke
 */
public class GetContainerInfo {

  /**
   * Parse information from a file, and also optionally print information about what
   * formats, containers and codecs fit into that file.
   * 
   * @param arg The file to open, or null if we just want generic options.
   * @throws IOException if file cannot be opened.
   * @throws InterruptedException if process is interrupted while querying.
   */
  private static void getInfo(String filename) throws InterruptedException, IOException {
   
    
    // In Humble, all objects have special contructors named 'make'.  
    // A Demuxer opens up media containers, parses  and de-multiplexes the streams
    // of media data without those containers.
    final Demuxer demuxer = Demuxer.make();
    
    // We open the demuxer by pointing it at a URL.
    demuxer.open(filename, null, false, true, null, null);
    
    // Once we've opened a demuxer, Humble can make a guess about the
    // DemuxerFormat. Humble supports over 100+ media container formats.
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

    String output = "/Users/lxb200709/Documents/TransCloud/videosource/elephants dream_00_cv.WebM";
    
    // we're forcing this to be HTTP Live Streaming for this demo.
    final Muxer muxer = Muxer.make(output, null, "mp4");
   
    
    //final MuxerFormat format_muxer = MuxerFormat.guessFormat("mp4", null, null);
    
    /**
     * Create bit stream filters if we are asked to.
     */
    final BitStreamFilter vf = BitStreamFilter.make("dump_extra");
    final BitStreamFilter af = BitStreamFilter.make("aac_adtstoasc");
    

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
    
    final MediaPacket packet = MediaPacket.make();
    ArrayList <MediaPicture> keyFrameList = new ArrayList<MediaPicture>();
    ArrayList <MediaPicture> keyFrameListInOnePacket = new ArrayList<MediaPicture>();
    ArrayList <String> frameList = new ArrayList<String>();
    ArrayList <Long> gopDuration = new ArrayList<Long>(); 
    long gopSize = 0; 
    long previousKeyFramePosition = 0;
    long currentKeyFramePosition = 0;
    long gopPts = 0;
    long previousKeyFramePts = 0;
    long currentKeyFramePts = 0;
    long gopDts = 0;
    long previousKeyFrameDts = 0;
    long currentKeyFrameDts = 0;
    long gopPosition = 0;
    

    int packetCount = 0;
        
    while(demuxer.read(packet) >= 0) {
      /**
       * Now we have a packet, but we can only write packets that had decoders we knew what to do with.
       */	
        final Decoder d = decoders[packet.getStreamIndex()];

      
      if(d != null && d.getCodecType() == Type.MEDIA_VIDEO) {  
    	  packetCount++;
          System.out.println("\npacket number: " + packetCount);
    	  System.out.println("packet position: " + packet.getPosition());
          System.out.println("packet duration: " + packet.getDuration());
          System.out.println("packet size: " + packet.getSize());
          System.out.println("packet dts: " + packet.getDts());
          System.out.println("packet pts: " + packet.getPts());
    	 System.out.println("this is a video packet");

	     picture = MediaPicture.make(
					              d.getWidth(),
					              d.getHeight(),
					              d.getPixelFormat());
	      picture.setTimeBase(demuxer.getStream(packet.getStreamIndex()).getFrameRate());
	      
	      int offset = 0;
	      int bytesDecoded = 0;
	      
	      while(offset < packet.getSize()){
	    	  bytesDecoded += d.decode(picture, packet, offset); 
	          if (bytesDecoded < 0) 
	              throw new RuntimeException("got error decoding video"); 
	          
	          offset += bytesDecoded; 
	
	    	  if(bytesDecoded >= 0) {      
	              if(picture.isComplete()){
	            	 	            	  
	            	  if(picture.getType() == MediaPicture.Type.PICTURE_TYPE_I) {
	            		  //Once a new GOP, create a new packet
		            	  final MediaPacket packetGOP = MediaPacket.make();
	            		  keyFrameList.add(picture);
	            		  keyFrameListInOnePacket.add(picture);
	            		  System.out.println("A I frame is created");
	            		  frameList.add("I");
	            		  
	            		  //Calculate GOP size I, the previous GOP size will current I frame position
	            		  //minus previous I frame position.
	            		  currentKeyFramePosition = packet.getPosition();
	            		  gopSize = currentKeyFramePosition - previousKeyFramePosition;
	            		  gopPosition = previousKeyFramePosition;
	            		  previousKeyFramePosition = currentKeyFramePosition;
	            		  
	            		  //Calculate GOP pts (deadline). It should the first frame in this GOP's pts, most time
	            		  //is key frame.
	            		  gopPts = previousKeyFramePts;
	            		  currentKeyFramePts = packet.getPts();
	            		  previousKeyFramePts = currentKeyFramePts;
	            		  
	            		  gopDts = previousKeyFrameDts;
	            		  currentKeyFrameDts = packet.getDts();
	            		  previousKeyFrameDts = currentKeyFrameDts;  
	            		  
	            		  /*packetGOP.setKeyPacket(true);
	            		  packetGOP.setTimeBase(packet.getTimeBase());
		            	  packetGOP.setDuration(gopDuration.size());
		            	  packetGOP.setPts(gopPts);
		            	  packetGOP.setDts(gopDts);
		            	  packetGOP.setPosition(gopPosition);
		            	//  packetGOP.setComplete(true);
		            	  
		            	  gopDuration.clear();
		            	  
		            	  if (vf != null && d.getCodecType() == Type.MEDIA_VIDEO)
		          	        vf.filter(packetGOP, null);
			      	      else if (af != null && d.getCodecType() == Type.MEDIA_AUDIO)
			      	        af.filter(packetGOP, null);
			          	  
			          	  System.out.println("*******Writing packetGOP to muxer container*****");*/
			      	      muxer.write(packet, true);
	                      }
	            	  if(picture.getType() == MediaPicture.Type.PICTURE_TYPE_P) {
	            		  System.out.println("A P frame is created");
	            		  frameList.add("P");

	            	  }
	            	  
	            	  if(picture.getType() == MediaPicture.Type.PICTURE_TYPE_B) {
	            		  System.out.println("A B frame is created");
	            		  frameList.add("B");

	            	  }
	                      
	            	  }
	            	  
	              }
	    	  }
	  }
      
	     
       
 /*     
      if(d.getCodecType() == Type.MEDIA_AUDIO) { 
    	  
    	  System.out.println("this is a audio packet");
    	  samples = MediaAudio.make(
				    			  d.getFrameSize(),
				    			  d.getSampleRate(),
				    			  d.getChannels(),
				    			  d.getChannelLayout(),
				    			  d.getSampleFormat());
    	  int offset = 0;
          int bytesRead = 0;
          do {
            bytesRead += d.decodeAudio(samples, packet, offset);
            if (samples.isComplete()) {
                 
            }
            offset += bytesRead;
          } while (offset < packet.getSize());
    	  
      }*/

      if (packet.isComplete() && d != null && d.getCodecType() == Type.MEDIA_VIDEO) {
          
    	  if(packet.isKeyPacket()){
    		  System.out.println("This is a keypacket");
    	  }
    	  
    	  //Calculate the total GOP duration 
    	  gopDuration.add(packet.getDuration());
    	  
    	  
	   //   System.out.printf("****Find %d I frames in this packet****", keyFrameListInOnePacket.size());
	      
	      //System.out.println("\n");

	      for(MediaPicture pic:keyFrameListInOnePacket) {
	          System.out.println("\nI frame #" + keyFrameListInOnePacket.indexOf(pic) + " pts: " + pic.getPts());
	      }
	      keyFrameListInOnePacket.clear();
	      System.out.println("\n");
	      
	      
    	 // System.out.println(frameList);
	     // System.out.println("\n");
      }
         
    }

    
    for (int i = 0; i < ns; i++) {
        do {
            decoders[i].decode(picture, null, 0);
            if (picture.isComplete()) {
             
            }
          } while (picture.isComplete());

     }
      // It is good practice to close demuxers when you're done to free
      // up file handles. Humble will EVENTUALLY detect if nothing else
      // references this demuxer and close it then, but get in the habit
      // of cleaning up after yourself, and your future girlfriend/boyfriend
      // will appreciate it.
      muxer.close();
      demuxer.close();
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
    double d = 1.0 * duration / Global.DEFAULT_PTS_PER_SECOND;
    int hours = (int) (d / (60*60));
    int mins = (int) ((d - hours*60*60) / 60);
    int secs = (int) (d - hours*60*60 - mins*60);
    int subsecs = (int)((d - (hours*60*60.0 + mins*60.0 + secs))*100.0);
    return String.format("%1$02d:%2$02d:%3$02d.%4$02d", hours, mins, secs, subsecs);
  }
  /**
   * Takes a media container (file) as the first argument, opens it,
   * and tells you what's inside the container.
   * 
   * @param args Must contain one string which represents a filename. If no arguments, then prints help.
   * @throws IOException if file cannot be opened.
   * @throws InterruptedException if process is interrupted while querying.
   */
  public static void main(String[] args) throws InterruptedException, IOException {
    
  //Print out everything in the console into the file
	printOutputFile pof = new printOutputFile();  
	pof.printOutToFile("BigBuckBunny_320x180_v7");  
	
	
	final Options options = new Options();
    options.addOption("h", "help", false, "displays help");
    options.addOption("v", "version", false, "version of this library");
    
    //OPen a file from local 
    // String filename = "/Users/lxb200709/Documents/TransCloud/videosource/big_buck_bunny_1080p_h264.mov";
   //  String filename = "/Users/lxb200709/Documents/TransCloud/videosource/sample.flv";
   // String filename = "/Users/lxb200709/Documents/TransCloud/videosource/bbb_sunflower_1080p_60fps_normal.mp4";

     String filename = "/Users/lxb200709/Documents/TransCloud/videosource/inputvideo/big_buck_bunny_720p_VP8_VORBIS_25fps_3900K.WebM";
   //  String filename = "/Users/lxb200709/Documents/TransCloud/videosource/big_buck_bunny_480p_h264.mov";
  //  String filename = "/Users/lxb200709/Documents/TransCloud/videosource/akiyo_cif.y4m";
    
     getInfo(filename);
    
    final CommandLineParser parser = new org.apache.commons.cli.BasicParser();
    try {
      final CommandLine cmd = parser.parse(options, args);
      if (cmd.hasOption("version")) {
        // let's find what version of the library we're running
        final String version = io.humble.video_native.Version.getVersionInfo();
        System.out.println("Humble Version: " + version);
      } else if (cmd.hasOption("help") || args.length == 0) {
        final HelpFormatter formatter = new HelpFormatter();
       // formatter.printHelp(GetContainerInfo.class.getCanonicalName() + " [<filename> ...]", options);
      } else {
        final String[] parsedArgs = cmd.getArgs();
        for(String arg: parsedArgs)
        getInfo(arg);
      }
    } catch (ParseException e) {
      System.err.println("Exception parsing command line: " + e.getLocalizedMessage());
    }
  }


}
