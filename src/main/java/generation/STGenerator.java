package generation;

import java.util.HashMap;
import java.util.Set;

public class STGenerator extends  Generatorx {
	public STGenerator(String obsFilePath, HashMap<String, String> associations, String urlIn, int scala, String startTime, int interval, Set<String> areaNames) {
		super(obsFilePath, associations, urlIn, scala, startTime, interval, areaNames);
	}
}
