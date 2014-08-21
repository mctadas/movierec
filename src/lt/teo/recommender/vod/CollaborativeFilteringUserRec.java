package lt.teo.recommender.vod;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class CollaborativeFilteringUserRec {

	public static void main(String[] args)
	{
		//recommend("data/train/vod_rated.csv", "data/pred/vod_rated.csv");
		recommend("data/train/mtvi_rated.csv", "data/pred/mtvi_rated.csv");
	}
	
	public static void recommend(String traincsv, String predcsv){
		try {
			
			DataModel dataModel = new FileDataModel(new File(traincsv));
		
			UserSimilarity similarity = new LogLikelihoodSimilarity(dataModel);
			//TanimotoCoefficientSimilarity similarity = new TanimotoCoefficientSimilarity(dataModel);
			
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(100, similarity, dataModel);
			
			GenericUserBasedRecommender recommender = new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(predcsv));
			
			int x = 0;
			for(LongPrimitiveIterator users = dataModel.getUserIDs(); users.hasNext();) {
				long userID = users.nextLong();
				List<RecommendedItem>recommendations = recommender.recommend(userID, 50);
				
				for(RecommendedItem recommendation : recommendations){
					bw.write(userID+","+recommendation.getItemID()+","+recommendation.getValue()+"\n");
				}
				x++;
				System.out.println(x + " " + userID);
			}
			bw.close();
				
		} catch (IOException e){
			System.out.println("I/O problems");
			e.printStackTrace();
		} catch (TasteException e) {
			System.out.println("recommender taste problems");
			e.printStackTrace();
		}
		
	}
}