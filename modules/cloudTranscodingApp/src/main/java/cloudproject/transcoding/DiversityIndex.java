package cloudproject.transcoding;

public class DiversityIndex {
	
	public static void main(String[] arg) {
    	  
    	  int[] s = new int[5];
    	  int total = 0;
    	  double[] pi = new double[5];
    	  double sum = 0.0;
    	  double H;
    	  double Hmax;
    	  double E;
    	  
    	  s[0] = 1;
    	  s[1] = 5;
    	  s[2] = 0;
    	  s[3] = 0;
    	  s[4] = 0;
    	  
    	  for(int i=0; i<5; i++){
    		   total += s[i];
    	  }
    	  
    	  for(int i=0; i<5; i++){
   		       pi[i] = s[i]*1.0/total; 
   	      }
    	  
    	  for(int i=0; i<5; i++){
    		  if(pi[i]>0){
  		       sum += pi[i]*Math.log(pi[i]);  
    		  }
  	      }
    	  
    	  H = 0-sum;
    	  Hmax = Math.log(5);
    	  E = H/Hmax;
    	  
    	  System.out.println("H = " + H);
    	  System.out.println("E = " + E);
    	  
    	  
      }
}
