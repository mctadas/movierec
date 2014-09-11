package lt.teo.recommender.future;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map;
import java.util.TreeSet;

import lt.teo.recommender.vod.CollaborativeFilteringUserRec;

public class ContentBasedRecommender {

	public static TreeMap<Integer, TreeMap<Integer, Double>> ratings;
	public static TreeMap<Integer, ArrayList<String>> features;
	public static TreeMap<Integer, TreeMap<String, Double>> weights;

	public static void main(String[] args) throws Exception {
		//loadTrainRatingsFromDBTable("IPTV_MTVI_rated_filtered_UAT");
		loadTrainRatingsFromFile("data/train/mtvi_rated.csv");

		String featuresSql = "select top 100 tv_id as ITEM "
				+ ",CASE WHEN [prodyear] = 0  THEN null ELSE ([prodyear]) END "
				+ ",CASE WHEN [category] = '' THEN null ELSE ([category]) END "
				+ ",CASE WHEN [genres] = ''   THEN null ELSE ([genres]) END "
				+ ",CASE WHEN [audience] = '' THEN null ELSE ([audience]) END "
				+ ",CASE WHEN [season] = 0    THEN null ELSE (CONVERT(varchar(20), [season])) END "
				+ ",CASE WHEN [episode] = 0   THEN null ELSE (CONVERT(varchar(20),[episode])) END "
				+ ",CASE WHEN [prad_laikas] < 0 THEN null ELSE (CONVERT(varchar(20),[prad_laikas])) END "
				+ "from [DWH_Darbinis].[dbo].[TV Programme]";
		loadItemFeaturesFromDBTable(featuresSql);
		computeWeights();
		System.out.println("DONE");
	}

	private static void computeWeights() {
		weights = new TreeMap<Integer, TreeMap<String, Double>>();

		//iterate through users
		for(Map.Entry<Integer, TreeMap<Integer, Double>> r_u : ratings.entrySet()) {
			Integer u = r_u.getKey();

			TreeMap<String, ArrayList<Integer>> featureMatrix = new TreeMap<String, ArrayList<Integer>>();
			TreeMap<Integer, Double> ratingMatrix = new TreeMap<Integer, Double>();

			//iterate through items
			for(Map.Entry<Integer, Double> r_ui : r_u.getValue().entrySet()) {
				Integer i = r_ui.getKey();
				Double r = r_ui.getValue();

				//create feature matrix
				for(String j: features.get(i)){

					ArrayList<Integer> fList = featureMatrix.get(j);
					if(fList == null){
						fList = new ArrayList<Integer>();
						featureMatrix.put(j, fList);
						fList = featureMatrix.get(j);
					}
					fList.add(i);

					ratingMatrix.put(i, r);
				}
			}

			//iterate through features
			for(Map.Entry<String, ArrayList<Integer>> f_iList : featureMatrix.entrySet())
			{
				String f_key = f_iList.getKey();
				double sum = 0.0;
				for(Integer i_key: f_iList.getValue())
				{
					sum += ratingMatrix.get(i_key);
				}
				Double weight = sum / f_iList.getValue().size();
				
				//store value to matrix
				TreeMap<String, Double> j_w = weights.get(u);
				if(j_w == null){
					j_w = new TreeMap<String, Double>();
					weights.put(u, j_w);
					j_w = weights.get(u);
				}
				j_w.put(f_key, weight);
			}
			System.out.println(weights);
			System.out.println(featureMatrix);
			System.out.println(ratingMatrix);
			
			
			
		}
	}



	public Object getFeatureVector(int u, Object featureMatrix) {

		// TODO Auto-generated method stub
		return null;
	}

	private static void loadItemFeaturesFromDBTable(String sql) throws Exception
	{
		features = new TreeMap<Integer, ArrayList<String>>();

		Connection conn = getDBConnection();
		Statement sta = conn.createStatement();
		String Sql = sql;

		ResultSet rs = sta.executeQuery(Sql);
		ResultSetMetaData meta = rs.getMetaData();
		final int columnCount = meta.getColumnCount();

		while (rs.next()) {
			Integer i = rs.getInt(1);
			for(int e=2; e<=columnCount;e++){
				String str = rs.getString(e);
				if(str != null){
					for(String val: str.split(",")) {
						addFeature(i, e+"_"+val);
					}
				}
			}
		}
		conn.close();
	}

	private static void addFeature(Integer i, String j)
	{
		ArrayList<String> i_j = features.get(i);
		if (i_j == null){
			i_j = new ArrayList<String>();
			features.put(i, i_j);
			i_j = features.get(i);
		}
		i_j.add(j);
	}

	public static void loadTrainRatingsFromDBTable(String trainTable) throws Exception
	{
		ratings = new TreeMap<Integer, TreeMap<Integer, Double>>();

		Connection conn = getDBConnection();
		Statement sta = conn.createStatement();
		String Sql = "SELECT * from "+trainTable;
		ResultSet rs = sta.executeQuery(Sql);
		while (rs.next()) {

			Integer u = rs.getInt(1);
			Integer i = rs.getInt(2);
			Double r = rs.getDouble(3);

			addRating(u,i,r);
		}
		conn.close();
	}
	
	public static void loadTrainRatingsFromFile(String path) throws Exception
	{
		ratings = new TreeMap<Integer, TreeMap<Integer, Double>>();
		
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line;
		while((line = br.readLine()) != null)
		{
			if(line.length()>0 &&
			   Character.isDigit(line.charAt(0))){
				String[] vals = line.split(",");
				addRating(Integer.valueOf(vals[0]),
						  Integer.valueOf(vals[1]),
						  Double.valueOf(vals[2]));
			}
		}
		br.close();
	}
	
	private static void addRating(Integer u, Integer i, Double r)
	{
		TreeMap<Integer, Double> u_i = ratings.get(u);
		if (u_i == null){
			u_i = new TreeMap<Integer, Double>();
			ratings.put(u, u_i);
			u_i = ratings.get(u);
		}
		u_i.put(i, r);
	}

	public static Connection getDBConnection() throws Exception
	{
		String userName = "dwh";
		String password = "vF_V8N26jCfi";

		String db_url = "jdbc:sqlserver://SRDWH\\CAVS;databaseName=EPDM";
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		Connection conn = DriverManager.getConnection(db_url, userName, password);

		return conn;
	}


}