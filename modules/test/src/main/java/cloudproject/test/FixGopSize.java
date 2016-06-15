package cloudproject.test;

import java.io.IOException;


public class FixGopSize {
	
		private static final int oWidth = 0;
		private static final int oHeight = 0;
		private static final int frameRate = 0;
		private static final int biteRate = 0;
		private static final String vcodec = null;
		private static final String ofmt = null;
		
		
		public static void main(String arg[]) throws IOException, InterruptedException{
			
			
			//Print out everything in the console into the file
		    printOutputFile pof = new printOutputFile();  
		    pof.printOutToFile("BigBuckBunny_320x180_v5"); 
		
			String inputUrl = "/Users/lxb200709/Documents/TransCloud/videosource/BigBuckBunny_320x180_short_cv.mp4";
			String outputUrl = "/Users/lxb200709/Documents/TransCloud/videosource/BigBuckBunny_320x180_short_cv2.mp4";
			
			
			//Transcoder ts = new Transcoder(inputUrl, outputUrl);
			Converter ts = new Converter(inputUrl, outputUrl);
			ts.setupStreams(oWidth, oHeight, frameRate, biteRate, vcodec, ofmt);
			ts.run(inputUrl, outputUrl);


	   }

	
}
