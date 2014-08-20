package lt.recommender.evaluate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class Evaluate {

	public static void main(String[] args) throws IOException {
		evaluate("data/pred/vod_rated.csv", "data/test/vod_rated.csv");
		evaluate("data/pred/vod_py_pred.csv", "data/test/vod_rated.csv");
	}
	
	public static void evaluate(String predDataPath, String testDataPath) throws IOException
	{
		System.out.println("---------\nEvaluated " + predDataPath+ " on "+testDataPath+"\n");
		
		BufferedReader brPred = new BufferedReader(new FileReader(predDataPath));
		BufferedReader brTest = new BufferedReader(new FileReader(testDataPath));

		//parse test data
		HashMap<String,Integer> testData = new HashMap<String, Integer>();
		String line;
		while((line = brTest.readLine()) != null)
		{
			String[] els = line.split(",");			
			testData.put(els[0]+','+els[1], Integer.parseInt(els[2]));		
		}

		//go through predictions to evaluate them
		int relevantRetrivedDocumentsCount = 0;
		int retrievedDocumentsCount = 0;
		int relevantDocumentsCount = 0;
		int hit = 0;

		while((line = brPred.readLine()) != null)
		{
			String[] els = line.split(",");
			String key = els[0]+','+els[1];
			
			if(testData.get(key)!=null){
				hit++;
				if(isRelevant(key, testData)){
					relevantRetrivedDocumentsCount++;
				}
			}
			
			retrievedDocumentsCount++;
		}
		
		//compute all relevant document in test set
		for(String key : testData.keySet()){
			if(isRelevant(key, testData)){
				relevantDocumentsCount++;
			}
		}

		double precision = relevantRetrivedDocumentsCount*1.0/hit;
		double recall = relevantRetrivedDocumentsCount*1.0/relevantDocumentsCount;
		double fMeasure = 2*(precision*recall/(precision+recall));
		double hitRate = hit*1.0/retrievedDocumentsCount;
		
		System.out.println("Precision:"+precision);
		System.out.println("Recall:"+recall);
		System.out.println("F1:"+fMeasure);
		System.out.println("HIT:"+hitRate);
		
		brPred.close();
		brTest.close();

	}
	
	private static boolean isRelevant(String key, HashMap<String,Integer> testData){
		Integer value = testData.get(key);
		if(value > 60){
			return true;
		} else {
			return false;
		}
	}

}
