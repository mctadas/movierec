package lt.teo.recommender.evaluate;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class RPT2CSV {

	/**
	 * replace "[ ]+" with ","
	 *  
	 * @param args
	 * @throws IOException 
	 */

	public static int testPercentile = 17;
	
	public static void main(String[] args) throws Exception
	{
//		convert("data/vod_data_rated",
//				"data/vod_rated.csv",
//				"data/train/vod_rated.csv",
//				"data/test/vod_rated.csv");
		
//		convert("data/mtvi_rated-7d.rpt",
//				"data/mtvi_rated.csv",
//				"data/train/mtvi_rated.csv",
//				"data/test/mtvi_rated.csv");
		
		convert2time("data/mtvi_rated-60d-2014.09.16.rpt",
				"data/mtvi_rated-60d-2014.09.16.csv",
				"data/train/mtvi_60d-rated.csv",
				"data/test/mtvi_60d-rated.csv", 
				"2014-09-09");
	}
	
	public static void convert(String datartp, String datacsv, String traincsv, String testcsv) throws IOException
	{
		BufferedReader br = new BufferedReader(new FileReader(datartp));
		BufferedWriter bw = new BufferedWriter(new FileWriter(datacsv));

		BufferedWriter bwTrain = new BufferedWriter(new FileWriter(traincsv));
		BufferedWriter bwTest = new BufferedWriter(new FileWriter(testcsv));
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
	
	public static void convert2time(String datartp, String datacsv, String traincsv, String testcsv, String time) throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader(datartp));
		BufferedWriter bw = new BufferedWriter(new FileWriter(datacsv));

		BufferedWriter bwTrain = new BufferedWriter(new FileWriter(traincsv));
		BufferedWriter bwTest = new BufferedWriter(new FileWriter(testcsv));
		String line;
		int trainCount = 1;
		int testCount = 1;
		
		
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
		Date date_split = sdf2.parse(time);
		
		while((line = br.readLine()) != null)
		{
			if(line.length()>0 &&
			   Character.isDigit(line.charAt(0))){
				
				String row = line.replaceAll("[ ]+",",");
								
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				String[] split = row.split(",");
				String dateStr = split[split.length-2];
				Date date = sdf.parse(dateStr);

				if(date.getTime() < date_split.getTime()){
					bwTrain.write(row+"\n");
					trainCount++;
				} else {
					bwTest.write(row+"\n");
					testCount++;
				}
				bw.write(row+"\n");
				//break;
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
