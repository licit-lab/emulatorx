package generation;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Set;

public class RTGenerator extends Generator {
	private final Logger log = LoggerFactory.getLogger(RTGenerator.class);


	public RTGenerator(String obsFilePath, HashMap<String, String> associations, String urlIn, int scala, String startTime, int interval, Set<String> areaNames) {
		super(obsFilePath, associations, urlIn, scala, startTime, interval, areaNames);
	}

	@Override
	public void run() {
		log.info("Launching the generator...");
		try {
			createAndSend(obsFilePath, scala, startTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void createAndSend(String obsFilePath, int scala, LocalDateTime startTime) throws InterruptedException {
		LocalDateTime previousTime = startTime;
		Reader reader;
		try {
			reader = Files.newBufferedReader(Paths.get(obsFilePath));
			CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';');
			CSVParser csvParser = csvFormat.parse(reader);
			LocalDateTime currentTime;
			for (CSVRecord r: csvParser) {
				currentTime = LocalDateTime.parse(r.get(3), formatter);
				long millisDiff = timestampsDifference(previousTime, currentTime, scala);
				log.info("Waiting {} ms before sending next sample", millisDiff);
				Thread.sleep(millisDiff); //Wait for a given scaled interval between two samples
				previousTime = currentTime;
				sendSample(r);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
