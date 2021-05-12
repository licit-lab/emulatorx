package generation;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class STGenerator extends Generator {
	private final Logger log = LoggerFactory.getLogger(STGenerator.class);


	public STGenerator(String obsFilePath, HashMap<String, String> associations, String urlIn, int scala, String startDateTime, int interval, Set<String> areaNames) {
		super(obsFilePath, associations, urlIn, scala, startDateTime, interval, areaNames);
	}

	@Override
	public void run() {
		log.info("Launching the STGenerator...");
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
			Iterator<CSVRecord> iterator = csvParser.iterator();

			while(iterator.hasNext()){
				previousDateTime = handleRecord(iterator.next(),previousDateTime,scala);
				if(iterator.hasNext())
					previousDateTime  = handleRecord(iterator.next(),previousDateTime,scala);
			}
			long fmillisDiff = timestampsDifference(endDateTime, previousDateTime, scala);
			log.info("Sleeping {} ms before sending final placeholder...", fmillisDiff);
			Thread.sleep(fmillisDiff);
			log.info("Sending final placeholder");
			sendPlaceHolder();
			log.info("Waiting 50 seconds to give time for the last aggregated packets to be sent by the area nodes"
					+ " to the remote broker...");
			Thread.sleep(50000);


		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private LocalDateTime handleRecord(CSVRecord record,LocalDateTime previousDateTime, int scala){
		LocalDateTime currentDateTime = LocalDateTime.parse(record.get(3),formatter);
		long millisDiff;
		if(currentDateTime.isAfter(endDateTime)){
			long diff1 = timestampsDifference(previousDateTime, endDateTime, scala);
			log.info("Sleeping {} ms before sending the placeholder...",diff1);
			try {
				Thread.sleep(diff1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			log.info("Sending placeholder");
			sendPlaceHolder();
			Duration duration =  Duration.between(currentDateTime, endDateTime);
			long diff = Math.abs(duration.toMinutes());
			int mul = (int) (diff/interval);
			log.info("The multiplier is {}", mul);
			mul++;
			millisDiff = super.timestampsDifference(endDateTime, currentDateTime, scala);
			updateDates(mul);
		} else
			millisDiff = timestampsDifference(previousDateTime, currentDateTime, scala);
		log.info("Waiting {} ms before sending next sample",millisDiff);
		try {
			Thread.sleep(millisDiff); //Wait for a given scaled interval between two samples
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		previousDateTime = currentDateTime;
		sendSample(record);
		return previousDateTime;
	}

	private void updateDates(int mul){
		this.startDateTime = this.startDateTime.plusMinutes(interval*mul);
		log.info("New startTime {}", startDateTime);
		this.endDateTime = this.startDateTime.plusMinutes(interval).minusSeconds(1);
		log.info("New endTime {}", endDateTime);
	}

	private void sendPlaceHolder(){
		for(String s: areaNames){
			SimpleString queue = new SimpleString(s);
			ClientProducer producer;
			try {
				producer = session.createProducer(queue);
				ClientMessage message = session.createMessage(true);
				message.putBooleanProperty("placeholder",true);
				log.info("Sending message {}",message.toString());
				producer.send(message);
				producer.close();
			} catch (ActiveMQException e) {
				e.printStackTrace();
			}
		}
	}
}
