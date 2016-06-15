package cloudproject.test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


public class TestTranscoder {
	
	
	
   public static void main(String arg[]) throws IOException, InterruptedException{
		
		
		//Print out everything in the console into the file
	    printOutputFile pof = new printOutputFile();  
	    pof.printOutToFile("BigBuckBunny_320x180_v4"); 
	
		String inputUrl = "inputvideo/big_buck_bunny_720p_MPEG2_MP2_25fps_3600K.MPG";
		String outputUrl = "outputvideo/big_buck_bunny_720p_MPEG2_MP2_25fps_3600K_cv.MPG";
		
		for(int i=0; i<10; i++){
	 	   System.out.println("\n#");
 		   transcodeTo320x180(inputUrl,outputUrl);
		}
   }
   
	
   public static void transcodeTo320x180(String inputUrl, String outputUrl) throws IOException, InterruptedException{
	   final int oWidth = 320;
	   final int oHeight = 180;
	   final int frameRate = 24;
	   final int biteRate = 0;
	   final String vcodec = null;
	   final String ofmt = null;
	   TreeMap<Integer, Long> gopTranscodingTimeMap = new TreeMap<Integer, Long>();
	   
	   Converter ts = new Converter(inputUrl, outputUrl);
	   ts.setupStreams(oWidth, oHeight, frameRate, biteRate, vcodec, ofmt);
	   gopTranscodingTimeMap = ts.run(inputUrl, outputUrl);
	   
	   PrintWriter pw = new PrintWriter(new FileWriter("datafile/big_buck_bunny_720p_MPEG2_MP2_25fps_3600K_data_v10.txt", true));
	   pw.printf("%-16s%-16s%-25s%-16s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
	   pw.println("\n");
	   
	   
		//System.out.format("%-16s%-16s%-16s%-25s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
		//System.out.println("\n");
	   int size = ts.getGopIdList().size();
	   for(int i=0; i<size; i++ ){
			/*System.out.format("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			System.out.println("\n");*/
			
			pw.printf("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			pw.println("\n");
		}
	   pw.close();
   }
   
   public static void transcodeTo640x480(String inputUrl, String outputUrl) throws IOException, InterruptedException{
	   final int oWidth = 640;
	   final int oHeight = 480;
	   final int frameRate = 24;
	   final int biteRate = 0;
	   final String vcodec = null;
	   final String ofmt = null;
	   TreeMap<Integer, Long> gopTranscodingTimeMap = new TreeMap<Integer, Long>();
	   
	   Converter ts = new Converter(inputUrl, outputUrl);
	   ts.setupStreams(oWidth, oHeight, frameRate, biteRate, vcodec, ofmt);
	   gopTranscodingTimeMap = ts.run(inputUrl, outputUrl);
	   
	   PrintWriter pw = new PrintWriter("/Users/lxb200709/Documents/TransCloud/outputPrint/BigBuckBunny_320x180_data_table.txt");
	   pw.printf("%-16s%-16s%-25s%-16s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
	   pw.println("\n");
	   
	   
		//System.out.format("%-16s%-16s%-16s%-25s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
		//System.out.println("\n");
	   int size = ts.getGopIdList().size();
	   for(int i=0; i<size; i++ ){
			/*System.out.format("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			System.out.println("\n");*/
			
			pw.printf("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			pw.println("\n");
		}
	   pw.close();  
   }
   
   public static void transcodeTo720x480(String inputUrl, String outputUrl) throws IOException, InterruptedException{
	   final int oWidth = 720;
	   final int oHeight = 480;
	   final int frameRate = 24;
	   final int biteRate = 0;
	   final String vcodec = null;
	   final String ofmt = null;
	   TreeMap<Integer, Long> gopTranscodingTimeMap = new TreeMap<Integer, Long>();
	   
	   Converter ts = new Converter(inputUrl, outputUrl);
	   ts.setupStreams(oWidth, oHeight, frameRate, biteRate, vcodec, ofmt);
	   gopTranscodingTimeMap = ts.run(inputUrl, outputUrl);
	   
	   PrintWriter pw = new PrintWriter("/Users/lxb200709/Documents/TransCloud/outputPrint/BigBuckBunny_320x180_data_table.txt");
	   pw.printf("%-16s%-16s%-25s%-16s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
	   pw.println("\n");
	   
	   
		//System.out.format("%-16s%-16s%-16s%-25s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
		//System.out.println("\n");
	   int size = ts.getGopIdList().size();
	   for(int i=0; i<size; i++ ){
			/*System.out.format("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			System.out.println("\n");*/
			
			pw.printf("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			pw.println("\n");
		}
	   pw.close();  
   }
   
   public static void transcodeTo720x576(String inputUrl, String outputUrl) throws IOException, InterruptedException{
	   final int oWidth = 720;
	   final int oHeight = 576;
	   final int frameRate = 24;
	   final int biteRate = 0;
	   final String vcodec = null;
	   final String ofmt = null;
	   TreeMap<Integer, Long> gopTranscodingTimeMap = new TreeMap<Integer, Long>();
	   
	   Converter ts = new Converter(inputUrl, outputUrl);
	   ts.setupStreams(oWidth, oHeight, frameRate, biteRate, vcodec, ofmt);
	   gopTranscodingTimeMap = ts.run(inputUrl, outputUrl);
	   
	   PrintWriter pw = new PrintWriter("/Users/lxb200709/Documents/TransCloud/outputPrint/BigBuckBunny_320x180_data_table.txt");
	   pw.printf("%-16s%-16s%-25s%-16s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
	   pw.println("\n");
	   
	   
		//System.out.format("%-16s%-16s%-16s%-25s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
		//System.out.println("\n");
	   int size = ts.getGopIdList().size();
	   for(int i=0; i<size; i++ ){
			/*System.out.format("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			System.out.println("\n");*/
			
			pw.printf("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			pw.println("\n");
		}
	   pw.close();  
   }
   
   public static void transcodeTo1280x720(String inputUrl, String outputUrl) throws IOException, InterruptedException{
	   final int oWidth = 1280;
	   final int oHeight = 720;
	   final int frameRate = 24;
	   final int biteRate = 0;
	   final String vcodec = null;
	   final String ofmt = null;
	   TreeMap<Integer, Long> gopTranscodingTimeMap = new TreeMap<Integer, Long>();
	   
	   Converter ts = new Converter(inputUrl, outputUrl);
	   ts.setupStreams(oWidth, oHeight, frameRate, biteRate, vcodec, ofmt);
	   gopTranscodingTimeMap = ts.run(inputUrl, outputUrl);
	   
	   PrintWriter pw = new PrintWriter("/Users/lxb200709/Documents/TransCloud/outputPrint/BigBuckBunny_320x180_data_table.txt");
	   pw.printf("%-16s%-16s%-25s%-16s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
	   pw.println("\n");
	   
	   
		//System.out.format("%-16s%-16s%-16s%-25s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
		//System.out.println("\n");
	   int size = ts.getGopIdList().size();
	   for(int i=0; i<size; i++ ){
			/*System.out.format("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			System.out.println("\n");*/
			
			pw.printf("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			pw.println("\n");
		}
	   pw.close();	  
   }
   
   public static void transcodeTo1920x1080(String inputUrl, String outputUrl) throws IOException, InterruptedException{
	   final int oWidth = 1920;
	   final int oHeight = 1080;
	   final int frameRate = 24;
	   final int biteRate = 0;
	   final String vcodec = null;
	   final String ofmt = null;
	   TreeMap<Integer, Long> gopTranscodingTimeMap = new TreeMap<Integer, Long>();
	   
	   Converter ts = new Converter(inputUrl, outputUrl);
	   ts.setupStreams(oWidth, oHeight, frameRate, biteRate, vcodec, ofmt);
	   gopTranscodingTimeMap = ts.run(inputUrl, outputUrl);
	   
	   PrintWriter pw = new PrintWriter("/Users/lxb200709/Documents/TransCloud/outputPrint/BigBuckBunny_320x180_data_table.txt");
	   pw.printf("%-16s%-16s%-25s%-16s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
	   pw.println("\n");
	   
	   
		//System.out.format("%-16s%-16s%-16s%-25s%-16s%-16s", "Resolution", "GOP#", "TranscodingTime", "Pts", "InputSize", "outputSize");
		//System.out.println("\n");
	   int size = ts.getGopIdList().size();
	   for(int i=0; i<size; i++ ){
			/*System.out.format("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			System.out.println("\n");*/
			
			pw.printf("%-16s%-16d%-25d%-16d%-16d%-16d", "320X180", ts.getGopIdList().get(i), ts.getGopTranscodingTimeList().get(i), 
					          ts.getGopPts().get(i), ts.getGopInputSize().get(i), ts.getGopOutputSize().get(i));
			pw.println("\n");
		}
	   pw.close();	  
   }

}
