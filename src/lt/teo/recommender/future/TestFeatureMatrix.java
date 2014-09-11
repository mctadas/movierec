package lt.teo.recommender.future;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class TestFeatureMatrix {

	private FeatureMatrix matrix;
	@Before
	public void setUp() throws Exception 
	{
		matrix = new FeatureMatrix();
	}
	
//	@Test
//	public void testGetNonExistingElement() {
//		assertNull(matrix.get(-1, -1, -1));
//	}
//	
//	@Test
//	public void testGetMatrixElement() {
//		matrix.add(1,2,3,4);
//		matrix.add(1,2,4,5);
//		matrix.add(1,3,4,6);
//		matrix.add(2,1,1,1);
//		
//		assertEquals(4, (int) matrix.get(1, 2, 3));
//		assertEquals(5, (int) matrix.get(1, 2, 4));
//		assertEquals(6, (int) matrix.get(1, 3, 4));
//		assertEquals(1, (int) matrix.get(2, 1, 1));
//	}

}
