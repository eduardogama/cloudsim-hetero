package cloudproject.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;


public class testPrintFile {
	
    public static void main(String[] arg) throws IOException {
    	
    	try{
    		PrintStream myconsole = new PrintStream(new File("/Users/lxb200709/Documents/TransCloud/outputPrint/example.txt"));
    		System.setOut(myconsole);
    		myconsole.println("Hello World");
    		
    	}catch(FileNotFoundException e){
    		 e.printStackTrace();
    	}

    }
}
