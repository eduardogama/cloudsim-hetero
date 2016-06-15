package cloudproject.test;

import java.util.Random;

public class Maintest {
	static double sd=1,mean=4;
	public static void main(String[] args) {
		Random r=new  Random(10);
		for (int i=0;i<10;++i){
		double v=r.nextGaussian()*sd+mean;
		System.out.println(v);
		}
	}
}
