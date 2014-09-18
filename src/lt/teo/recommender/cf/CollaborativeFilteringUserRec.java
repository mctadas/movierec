package lt.teo.recommender.cf;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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

	public static String userName;
	public static String password;
	public static String db_url;
	
	public static void main(String[] args) throws Exception
	{
		String trainTable = null;
		String predTable = null;
		int topN = 0;

		if (args.length < 5) {
			System.out.println("[USAGE]\n noOfPredictionsToCompute trainingTable destPredictionTable DB_user DB_pass DB_url\n");
			System.exit(-1);
		} else {
			topN = Integer.parseInt(args[0]); //50;
			trainTable = args[1]; //"IPTV_VOD_rated_filtered_UAT";
			predTable =args[2]; //"IPTV_VOD_prediction_UAT";
			userName = args[3];//"dwh"
			password = args[4];//"vF_V8N26jCfi";
			db_url = args[5];//"jdbc:sqlserver://SRDWH\\CAVS;databaseName=EPDM"
		}
		
		Path tempFile = getTrainFileFromDBTable(trainTable);

		recommend(tempFile.toString(), predTable, topN);

		deleteFile(tempFile.toString());
		System.out.println("DONE");
	}

	public static Path getTrainFileFromDBTable(String trainTable) throws Exception
	{
		Connection conn = getDBConnection();
		Statement sta = conn.createStatement();
		String Sql = "SELECT * from "+trainTable;
		Path tempFile = Files.createTempFile(trainTable, ".tmp");

		ResultSet rs = sta.executeQuery(Sql);
		List<String> rows = new ArrayList<String>();
		while (rs.next()) {
			String str = rs.getInt(1)+","+rs.getInt(2)+","+rs.getFloat(3);
			rows.add(str);		
		}

		Files.write(tempFile, rows, Charset.defaultCharset(), StandardOpenOption.WRITE);
		System.out.printf("Wrote text to temporary file %s%n", tempFile.toString());

		return tempFile;
	}
	
	public static void recommend(String trainFile, String predcsv, int topN) throws Exception
	{
		// build recommender model
		DataModel dataModel = new FileDataModel(new File(trainFile));
		UserSimilarity similarity = new LogLikelihoodSimilarity(dataModel);
		//TanimotoCoefficientSimilarity similarity = new TanimotoCoefficientSimilarity(dataModel);
		UserNeighborhood neighborhood = new NearestNUserNeighborhood(100, similarity, dataModel);
		GenericUserBasedRecommender recommender = new GenericUserBasedRecommender(dataModel, neighborhood, similarity);

		//compute predictions on created model
		Connection conn = getDBConnection();
		int x = 0;
		for(LongPrimitiveIterator users = dataModel.getUserIDs(); users.hasNext();) {
			long userID = users.nextLong();
			List<RecommendedItem> recommendations = recommender.recommend(userID, topN);

			for(RecommendedItem recommendation : recommendations){
				String sql = "INSERT INTO " + predcsv +" VALUES (?,?,?,?)";
				PreparedStatement prep = conn.prepareStatement(sql);
				prep.setInt(1, (int) userID);
				prep.setInt(2, (int) recommendation.getItemID());
				prep.setFloat(3, recommendation.getValue());
				prep.setTimestamp(4, getCurrentTimeStamp());
				prep.executeUpdate();
			}
			x++;
			System.out.println(x + " " + userID);
		}
	}

	public static java.sql.Timestamp getCurrentTimeStamp()
	{
		java.util.Date today = new java.util.Date();
		return new java.sql.Timestamp(today.getTime());
	}
	
	public static void deleteFile(String path)
	{
		java.io.File f = new java.io.File(path);
		f.delete();
	}
	
	public static Connection getDBConnection() throws Exception
	{
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		Connection conn = DriverManager.getConnection(db_url, userName, password);
		return conn;
	}
}