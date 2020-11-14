package generation;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.SettingReader;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class Generatorx extends Thread {
	private int scala;
	private LocalDateTime startTime;
	private LocalDateTime finalTime;
	private HashMap<String, String> associations; //It maintains associations between links and areas
	private final Logger log = LoggerFactory.getLogger(Generatorx.class);
	private ClientSession session;
	private int interval;
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
	private Set<String> areaNames;
	private String obsFilePath;

	public Generatorx(String obsFilePath,HashMap<String, String> associations, String urlIn, int scala, String startTime,
					  int interval, Set<String> areaNames){
		this.associations = associations;
		this.scala = scala;
		this.startTime = LocalDateTime.parse(startTime,formatter);
		this.finalTime = this.startTime.plusMinutes(interval);
		this.interval = interval;
		this.areaNames = areaNames;
		this.obsFilePath = obsFilePath;
		ClientSessionFactory factory;
		this.session = null;
		try {
			ServerLocator locator = ActiveMQClient.createServerLocator(urlIn);
			factory = locator.createSessionFactory();
			session = factory.createSession(true,true);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
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
			Iterator<CSVRecord> iterator = csvParser.iterator();

			while(iterator.hasNext()){
				previousTime = handleRecord(iterator.next(),previousTime,scala);
				if(iterator.hasNext())
					previousTime  = handleRecord(iterator.next(),previousTime,scala);
			}
			log.info("Sending final placeholder");
			sendPlaceHolder();
			log.info("Waiting 10 seconds to give time for the last aggregated packets to be sent by the area nodes"
			+ " to the remote broker...");
			Thread.sleep(10000);

			/*for (CSVRecord r: csvParser){
				currentTime = LocalDateTime.parse(r.get(3),formatter);
				long millisDiff = timestampsDifference(previousTime, , scala);
				log.info("Waiting {} ms before sending next sample",millisDiff);
				Thread.sleep(millisDiff); //Wait for a given scaled interval between two samples
				previousTime = currenTime;
				if(.isEqual(finalTime) || .isAfter(finalTime)) {
					log.info("");
					sendPlaceHolder();
					updateDates();
					this.sleep(2500);
				}
				sendSample(r);
			}*/
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private LocalDateTime handleRecord(CSVRecord record,LocalDateTime previousTime, int scala){
		LocalDateTime currentTime = LocalDateTime.parse(record.get(3),formatter);
		long millisDiff;
		if(currentTime.isEqual(finalTime) || currentTime.isAfter(finalTime)){
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
			millisDiff = timestampsDifference(finalTime, currentTime, scala);
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

	private long timestampsDifference(LocalDateTime previous, LocalDateTime current, int scala) {
		return Math.abs(Duration.between(previous,current).toMillis()/scala);
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

	private void sendSample(CSVRecord r){
		SimpleString queue = new SimpleString(associations.get(r.get(1)));
		ClientProducer producer;
		try {
			producer = session.createProducer(queue);
			ClientMessage message = session.createMessage(true);
			message.putLongProperty("linkid", Long.parseLong(r.get(1)));
			message.putFloatProperty("coverage", Float.parseFloat(r.get(2).replace(',','.')));
			message.putStringProperty("timestamp", r.get(3));
			message.putFloatProperty("speed", Float.parseFloat(r.get(4)));
			message.putBooleanProperty("placeholder",false);
			log.info("Sending message {}",message.toString());
			producer.send(message);
			producer.close();
		} catch (ActiveMQException e) {
			e.printStackTrace();
		}
	}

	private void updateDates(int mul){
		this.startTime = this.startTime.plusMinutes(interval*mul);
		log.info("New startTime {}",startTime);
		this.finalTime = this.startTime.plusMinutes(interval);
		log.info("New endTime {}",finalTime);
	}

}
