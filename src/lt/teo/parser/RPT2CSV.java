package lt.teo.parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class RPT2CSV {

	/**
	 * replace "[ ]+" with ","
	 *  
	 * @param args
	 * @throws IOException 
	 */

	public static int testPercentile = 17;
	
	public static void main(String[] args) throws IOException
	{
		BufferedReader br = new BufferedReader(new FileReader("data/vod_data_rated"));
		BufferedWriter bw = new BufferedWriter(new FileWriter("data/vod_rated.csv"));

		BufferedWriter bwTrain = new BufferedWriter(new FileWriter("data/train/vod_rated.csv"));
		BufferedWriter bwTest = new BufferedWriter(new FileWriter("data/test/vod_rated.csv"));
		Random randomGenerator = new Random();
		String line;
		int trainCount = 1;
		int testCount = 1;
		
		
		while((line = br.readLine()) != null)
		{
			if(line.length()>0 &&
			   Character.isDigit(line.charAt(0))){
				
				String row = line.replaceAll("[ ]+",",");
				if(randomGenerator.nextInt(100) < (100-testPercentile)){
					bwTrain.write(row+"\n");
					trainCount++;
				}else{
					bwTest.write(row+"\n");
					testCount++;
				}
				bw.write(row+"\n");
			}
		}

		br.close();
		bw.close();
		bwTrain.close();
		bwTest.close();
		System.out.println("train:" + trainCount + " test:" + testCount + " test %:"+(testCount*1.0)/trainCount);
		System.out.println("DONE");
	}

}
