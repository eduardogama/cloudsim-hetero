package cloudproject.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ParseTable {
	
	public ParseTable(){
		
	}
	
	public static void  main(String arg[]){
		BufferedReader br = null;
	    String[] characters = new String[1024];
	    int[] gopTranscodingTime = new int[1000];
	
	    try {
	
	        String sCurrentLine;
	        br = new BufferedReader(new FileReader("/Users/lxb200709/Documents/TransCloud/outputPrint/BigBuckBunny_320x180_data_table.txt"));
	       
	        int i=0;
	        int num = 0;
	        while ((sCurrentLine = br.readLine()) != null) {
	            String[] arr = sCurrentLine.split("\\s+");
	            
	          //  System.out.println("the length is:" + arr.length);
	            //for the first line it'll print
	         if(sCurrentLine.length() > 0){
	            /*System.out.println("arr[0] = " + arr[0]); // h
	            System.out.println("arr[1] = " + arr[1]); // Vito
	            System.out.println("arr[2] = " + arr[2]); // 123
	            System.out.println("arr[3] = " + arr[3]); // 123
	            System.out.println("arr[4] = " + arr[4]); // 123
	            System.out.println("arr[5] = " + arr[5]); // 123
*/
	         
	         //   System.out.println(sCurrentLine);
	            //Now if you want to enter them into separate arrays
	         //   characters[i] = arr[0];
	            // and you can do the same with
	            // names[1] = arr[1]
	            //etc
	            if(i > 0){
	            	
	            	gopTranscodingTime[num] = Integer.parseInt(arr[2]);
	            	num++;
	            	
	            }
	            i++;
	          }
	        }
	       int count = i;
	        
	        
	        for(i=0; i< count - 1; i++){
	           System.out.println(gopTranscodingTime[i] + " ");
	        }
	
	    } catch (IOException e) {
	        e.printStackTrace();
	    } finally {
	        try {
	            if (br != null)br.close();
	        } catch (IOException ex) {
	            ex.printStackTrace();
	        }
	    }
	}

}
