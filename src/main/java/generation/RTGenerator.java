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


	public RTGenerator(String obsFilePath, HashMap<String, String> associations, String urlIn, int scala, String startDateTime, int interval, Set<String> areaNames) {
		super(obsFilePath, associations, urlIn, scala, startDateTime, interval, areaNames);
	}

	@Override
	public void run() {
		log.info("Launching the RTGenerator...");
		try {
			createAndSend(obsFilePath, scala, startDateTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void createAndSend(String obsFilePath, int scala, LocalDateTime startDateTime) throws InterruptedException {
		LocalDateTime previousDateTime = startDateTime;
		Reader reader;
		try {
			reader = Files.newBufferedReader(Paths.get(obsFilePath));
			CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';');
			CSVParser csvParser = csvFormat.parse(reader);
			LocalDateTime currentDateTime;
			for (CSVRecord r: csvParser) {
				currentDateTime = LocalDateTime.parse(r.get(3), formatter);
				long millisDiff = timestampsDifference(previousDateTime, currentDateTime, scala);
				log.info("Waiting {} ms before sending next sample", millisDiff);
				Thread.sleep(millisDiff); //Wait for a given scaled interval between two samples
				previousDateTime = currentDateTime;
				sendSample(r);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
