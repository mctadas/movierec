package lt.teo.recommender.future;

import java.util.TreeMap;

public class FeatureMatrix 
{
	public TreeMap<Integer, TreeMap<String, Integer>> matrix;

	public FeatureMatrix(){
		matrix = new TreeMap<Integer, 
				TreeMap<String, Integer>>();
	}

	public Integer get(int i, int j) 
	{
		try {
			return matrix.get(i).get(j);
		}catch (Exception e) {
			return null;
		}
	}
	
	public void add(Integer i, String j, Integer value) 
	{
		TreeMap<String, Integer> m_i = matrix.get(i);
		if(m_i == null){
			m_i = new TreeMap<String, Integer>();
			matrix.put((Integer)i, m_i);
			m_i = matrix.get(i);
		}
		m_i.put(j, (Integer) value);
	}

}
