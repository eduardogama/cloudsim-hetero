package cloudproject.transcoding;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;

public class CreateHetergeneousData {
    
	   public static void main(String args[]) throws IOException{
		   
		   
		   
	       File inputfolder = new File("/Users/lxb200709/Documents/TransCloud/amazon_aws/instances/t2.small/outputdata");
	       File outputfolder = new File("/Users/lxb200709/Documents/TransCloud/amazon_aws/instances/outputdata");
	       
           
	       File[] listOfFiles = inputfolder.listFiles();
	 	  
	 	   for (int i = 0; i < listOfFiles.length; i++) {
	 		  File inputfile = listOfFiles[i];
			  String name = inputfile.getName().toLowerCase();

	 		  if (inputfile.isFile() && inputfile.getName().endsWith(".txt")) {
	 			  
	 			//find file basic name 
				 String fileName = FilenameUtils.getBaseName(name);
				 String extension = FilenameUtils.getExtension(name);
	 			  
				//define output .txt file
			     String datafile = fileName + ".txt";
			     File data = new File(outputfolder, datafile);
			     String outputData = data.getAbsolutePath();

			     PrintWriter pw = new PrintWriter(new FileWriter(outputData, true));
			     pw.printf("%-25s%-16s%-16s%-25s%-16s%-16s%-16s", "TranscodingType", "EC2Type", "GOP#", "TranscodingTime", "Pts", "InputSize", "OutputSize");
			     pw.println("\n");
			     
			     /**
				 * Combine 10 times reuslte to one document
				 */
			     ParseData pd = new ParseData();
	 			 pd.parseData(inputfile); 
	 			 
	 			 Random rd = new Random();
			     
	 			for(Integer id:pd.getGopIdList()){
	 				/*int max = pd.getGopTranscodingTimeList().get(id-1);
	 				int var = rd.nextInt(max + 1) - max/2 ;*/
	 				
					pw.printf("%-25s%-16s%-16d%-25d%-16d%-16d%-16d", "Resolution", "t2.small", id, pd.getGopTranscodingTimeList().get(id-1), 
							          pd.getGopPtsList().get(id-1), pd.getGopInputSizeList().get(id-1), pd.getGopOutputSizeList().get(id-1));
					pw.println("\n");
				 }
				pw.close();  
	 			  
	 		  } 
	 	   }	 
		   
	   }

	
}
