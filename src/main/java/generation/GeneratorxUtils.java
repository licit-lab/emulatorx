package generation;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class GeneratorxUtils {
	private static final Logger log = LoggerFactory.getLogger(GeneratorxUtils.class);

	private static long timestampsDifference(String previous, String current, int scala) {
		SimpleDateFormat fmt = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date previousDate = null;
		Date currentDate = null;
		try {
			previousDate = fmt.parse(previous);
			currentDate = fmt.parse(current);
		} catch (ParseException e) {
			log.error("Error in parsing data: " + e);
		}

		long millisDiff = currentDate.getTime() - previousDate.getTime();
		return millisDiff/scala;
	}

	static void createAndSend(ClientSession session, String obsFilePath, int scala, HashMap<String, String> associations,
							  String startTime) throws InterruptedException {
		String previousTmp = startTime;
		String currentTmp;
		Reader reader;
		try {
			reader = Files.newBufferedReader(Paths.get(obsFilePath));
			CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';');
			CSVParser csvParser = csvFormat.parse(reader);
			for (CSVRecord r: csvParser){
				currentTmp=r.get(3);
				long millisDiff = timestampsDifference(previousTmp, currentTmp, scala);
				log.info("Waiting {} ms before sending next sample",millisDiff);
				Thread.sleep(millisDiff); //Wait for a given scaled interval between two samples
				previousTmp = currentTmp;
				SimpleString queue = new SimpleString(associations.get(r.get(1)));
				ClientProducer producer;
				try {
					producer = session.createProducer(queue);
					ClientMessage message = session.createMessage(true);
					message.putLongProperty("linkid", Long.parseLong(r.get(1)));
					message.putFloatProperty("coverage", Float.parseFloat(r.get(2).replace(',','.')));
					message.putStringProperty("timestamp", r.get(3));
					message.putFloatProperty("speed", Float.parseFloat(r.get(4)));
					log.info("Sending message {}",message.toString());
					producer.send(message);
				} catch (ActiveMQException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}