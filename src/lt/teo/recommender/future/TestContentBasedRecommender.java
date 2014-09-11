package lt.teo.recommender.future;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

public class TestContentBasedRecommender {

	private ContentBasedRecommender rec;
	private FeatureMatrix featureMatrix;
	private List<Double> ratingVector;
	
	private String doubleToInt(double value) {
		return String.format("%.2f", value);
	}
	
//	@Before
//	public void setUp() throws Exception 
//	{
//		rec = new ContentBasedRecommender();
//		
//		featureMatrix = new FeatureMatrix();
//		featureMatrix.add(0,0,"0",1);
//		featureMatrix.add(0,1,"0",0);
//		featureMatrix.add(0,2,"0",1);
//		featureMatrix.add(0,3,"0",1);
//		featureMatrix.add(0,4,"0",0);
//		featureMatrix.add(0,5,"0",1);
//		featureMatrix.add(0,6,"0",1);
//		featureMatrix.add(0,7,"0",0);
//		featureMatrix.add(0,8,"0",1);
//		
//		featureMatrix.add(0,0,"0",1);
//		featureMatrix.add(0,1,"0",1);
//		featureMatrix.add(0,2,"0",1);
//		featureMatrix.add(0,3,"0",0);
//		featureMatrix.add(0,4,"0",0);
//		featureMatrix.add(0,5,"0",0);
//		featureMatrix.add(0,6,"0",0);
//		featureMatrix.add(0,7,"0",1);
//		featureMatrix.add(0,8,"0",0);
//		
//		featureMatrix.add(1,0,"0",0);
//				
//	
//		double[] initRatings= {0.5, 0.3, 0.9, 0.7, 0.2, 1.0, 0.44, 0.67, 0.2};
//		ratingVector = new ArrayList<Double>();
//		for(Double rating: initRatings){
//			ratingVector.add(rating);
//		}
//	}
//
//	@Test
//	public void testGetFeatureWeight() {
//		double weight = rec.getFeatureWeight(0, 0, featureMatrix, ratingVector);
//		assertEquals("0,42", doubleToInt(weight));
//	}
//	
//	@Test
//	public void testFeatureMatrixFormat() {
//		assertEquals("{0={0=1, 1=1}, 1={0=0, 1=1}, 2={0=1, 1=1}, 3={0=1, 1=0}, 4={0=0, 1=0}, 5={0=1, 1=0}, 6={0=1, 1=0}, 7={0=0, 1=1}, 8={0=1, 1=0}}",
//		     	 featureMatrix.getPart(0).toString());
//		assertEquals("{0={0=0}}",
//				 featureMatrix.getPart(1).toString());
//	}
//	
//	@Test
//	public void testFeatureVector() {
//		int userId = 0;
//		Object obj = rec.getFeatureVector(userId, featureMatrix);
//		fail("not implemented");
//	}
//	
//	@Test
//	public void testGetFeatureVector() {
//		int userId = 0;
//		Object obj = rec.getFeatureVector(userId, featureMatrix);
//		fail("not implemented");
//	}
	

	
}
