
package cloudproject.test;


/*******************************************************************************
 * Copyright (c) 2008, 2010 Xuggle Inc.  All rights reserved.
 *  
 * This file is part of Xuggle-Xuggler-Main.
 *
 * Xuggle-Xuggler-Main is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Xuggle-Xuggler-Main is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Xuggle-Xuggler-Main.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/

import java.io.IOException;
import java.util.ArrayList;


/**
 * Opens up a media container, and prints out a summary of the contents.
 * 
 * If you pass -Dxuggle.options we'll also tell you what every
 * configurable option on the container and streams is set to.
 * 
 * @author aclarke
 *
 */
public class ReadContainer{
	
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
  
  
  
  /**
   * Takes a media container (file) as the first argument, opens it, and tells you what's inside the container.
   * @param args Must contain one string which represents a filename
 * @throws IOException 
   */
  public static void main(String[] args) throws IOException
  {
    // If the user passes -Dxuggle.options, then we print
    // out all possible options as well.
   /* String optionString = System.getProperty("xuggle.options");
    if (optionString != null)
    {
      Configuration.printHelp(System.out);
    }

    if (args.length <= 0)
      throw new IllegalArgumentException("must pass in a filename as the first argument");
*/    
    String inputFilename = "/Users/lxb200709/Documents/TransCloud/videosource/inputvideo/elephants dream_04.ts";

  //Print out everything in the console into the file
  	printOutputFile pof = new printOutputFile();  
  	pof.printOutToFile("BigBuckBunny_320x180_v5");  
	  
	//Create a Xuggler container object
    IContainer demuxerContainer = IContainer.make();
   
    // Open up the demuxer container
    int retvalDemuxer = demuxerContainer.open(inputFilename, IContainer.Type.READ, null); 
    if (retvalDemuxer < 0)
      throw new IllegalArgumentException("could not open file: " + inputFilename);
    
    
    // query how many streams the call to open found
    int numStreams = demuxerContainer.getNumStreams();
    System.out.printf("file \"%s\": %d stream%s; ",
        inputFilename,
        numStreams,
        numStreams == 1 ? "" : "s");
   // System.out.println("duration: " + demuxerContainer.getDuration());
    final String formattedDuration = formatTimeStamp(demuxerContainer.getDuration());
    System.out.printf("\nDuration: %s, start: %f, bitrate: %d kb/s\n",
        formattedDuration,
        demuxerContainer.getStartTime() == Global.NO_PTS ? 0 : demuxerContainer.getStartTime() / 1000000.0,
            demuxerContainer.getBitRate()/1000);
    
    int videoStreamIndex = 0;
    int audioStreamIndex = 0;

   // and iterate through the streams to print their meta data
    for(int i = 0; i < numStreams; i++)
    {
      // Find the stream object
      IStream streamIn = demuxerContainer.getStream(i);
      // Get the pre-configured decoder that can decode this stream;
      IStreamCoder coderIn = streamIn.getStreamCoder();
      
      // and now print out the meta data.
      System.out.printf("\n  stream %d: ",    i);
      System.out.printf("type: %s; ",     coderIn.getCodecType());
      System.out.printf("\n            codec: %s; ",    coderIn.getCodecID());
      System.out.printf("\n            GOP number is: %d", coderIn.getNumPicturesInGroupOfPictures());
   //   System.out.printf("\n            duration: %s; ", streamIn.getDuration() == Global.NO_PTS ? "unknown" : "" + streamIn.getDuration());

      final String formattedStreamDuration = formatStreamTimeStamp(streamIn, streamIn.getDuration());
      System.out.printf("\n            duration: %s; ", streamIn.getDuration() == Global.NO_PTS ? "unknown" : "" + formattedStreamDuration);
      System.out.printf("\n            start time: %s; ", demuxerContainer.getStartTime() == Global.NO_PTS ? "unknown" : "" + streamIn.getStartTime());
      System.out.printf("\n            language: %s; ", streamIn.getLanguage() == null ? "unknown" : streamIn.getLanguage());
      System.out.printf("\n            timebase: %d/%d; ", streamIn.getTimeBase().getNumerator(), streamIn.getTimeBase().getDenominator());
      System.out.printf("\n            coder tb: %d/%d; ", coderIn.getTimeBase().getNumerator(), coderIn.getTimeBase().getDenominator());
      
      if (coderIn.getCodecType() == ICodec.Type.CODEC_TYPE_AUDIO)
      {
        System.out.printf("\n            sample rate: %d; ", coderIn.getSampleRate());
        System.out.printf("\n            channels: %d; ",    coderIn.getChannels());
        System.out.printf("\n            format: %s",        coderIn.getSampleFormat());
        System.out.printf("\n");
        audioStreamIndex = i;
        
      } else if (coderIn.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO)
      {
        System.out.printf("\n            width: %d; ",  coderIn.getWidth());
        System.out.printf("\n            height: %d; ", coderIn.getHeight());
        System.out.printf("\n            format: %s; ", coderIn.getPixelType());
        System.out.printf("\n            frame-rate: %5.2f; ", coderIn.getFrameRate().getDouble());
        System.out.println("\n            frame number: " + streamIn.getNumFrames());
        System.out.println("            bite rate: " + coderIn.getBitRate()/1000 + " kb/s");

        System.out.printf("\n");
        videoStreamIndex = i;
      }
    }
    
    IPacket packetIn = IPacket.make();
    int videoPacketCount = 0;
    int packetCount = 0;
    
    while (demuxerContainer.readNextPacket(packetIn) >= 0) {
      packetCount++;
      /*System.out.println("\npacket number: " + packetCount);
  	  System.out.println("packet position: " + packetIn.getPosition());
      System.out.println("packet duration: " + packetIn.getDuration());
      System.out.println("packet size: " + packetIn.getSize());
      System.out.println("packet dts: " + packetIn.getDts());
      System.out.println("packet pts: " + packetIn.getPts()); */
       
      
      if(packetIn.getStreamIndex() == videoStreamIndex) {     
       videoPacketCount++;
       System.out.println("\nvideo packet number: " + videoPacketCount);   
       System.out.println("  stream index: " + packetIn.getStreamIndex());
       System.out.println("  key: " + packetIn.isKey());
       System.out.println("  position: " + packetIn.getPosition());
      // System.out.println("  duration: " + packetIn.getDuration());

       
       final String formattedPacketDuration = formatPacketTimeStamp(packetIn, packetIn.getDuration());
       System.out.println("  duration: " + formattedPacketDuration);
       System.out.println("  size: " + packetIn.getSize());
       System.out.println("  dts: " + formatPacketTimeStamp(packetIn, packetIn.getDts()));
       System.out.println("  pts: " + formatPacketTimeStamp(packetIn, packetIn.getPts()));
       System.out.println("  pts: " + packetIn.getPts());
       
       if(packetIn.isKeyPacket()){
    	   System.out.println("this is a key packet");
       }

   
	    }
    }
    
    System.out.printf("\n*************The Simulation is End************\n");
  }
	
}

