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


	public STGenerator(String obsFilePath, HashMap<String, String> associations, String urlIn, int scala, String startTime, int interval, Set<String> areaNames) {
		super(obsFilePath, associations, urlIn, scala, startTime, interval, areaNames);
	}

	@Override
	public void run() {
		log.info("Launching the STGenerator...");
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
			Iterator<CSVRecord> iterator = csvParser.iterator();

			while(iterator.hasNext()){
				previousTime = handleRecord(iterator.next(),previousTime,scala);
				if(iterator.hasNext())
					previousTime  = handleRecord(iterator.next(),previousTime,scala);
			}
			long fmillisDiff = timestampsDifference(finalTime, previousTime, scala);
			log.info("Sleeping {} ms before sending final placeholder...", fmillisDiff);
			Thread.sleep(fmillisDiff);
			log.info("Sending final placeholder");
			sendPlaceHolder();
			log.info("Waiting 10 seconds to give time for the last aggregated packets to be sent by the area nodes"
					+ " to the remote broker...");
			Thread.sleep(10000);


		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private LocalDateTime handleRecord(CSVRecord record,LocalDateTime previousTime, int scala){
		LocalDateTime currentTime = LocalDateTime.parse(record.get(3),formatter);
		long millisDiff;
		if(currentTime.isAfter(finalTime)){
			long diff1 = timestampsDifference(previousTime, finalTime, scala);
			log.info("Sleeping {} ms before sending the placeholder...",diff1);
			try {
				Thread.sleep(diff1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			log.info("Sending placeholder");
			sendPlaceHolder();
			Duration duration =  Duration.between(currentTime,finalTime);
			long diff = Math.abs(duration.toMinutes());
			int mul = (int) (diff/interval);
			log.info("The multiplier is {}", mul);
			mul++;
			millisDiff = super.timestampsDifference(finalTime, currentTime, scala);
			updateDates(mul);
		} else
			millisDiff = timestampsDifference(previousTime, currentTime, scala);
		log.info("Waiting {} ms before sending next sample",millisDiff);
		try {
			Thread.sleep(millisDiff); //Wait for a given scaled interval between two samples
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		previousTime = currentTime;
		sendSample(record);
		return previousTime;
	}

	private void updateDates(int mul){
		this.startTime = this.startTime.plusMinutes(interval*mul);
		log.info("New startTime {}",startTime);
		this.finalTime = this.startTime.plusMinutes(interval).minusSeconds(1);
		log.info("New endTime {}",finalTime);
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
