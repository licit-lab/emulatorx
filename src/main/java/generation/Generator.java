package generation;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Set;

public class Generator extends Thread {
	protected int scala;
	protected LocalDateTime startTime;
	protected LocalDateTime finalTime;
	protected HashMap<String, String> associations; //It maintains associations between links and areas
	private final Logger log = LoggerFactory.getLogger(Generator.class);
	protected ClientSession session;
	protected int interval;
	protected DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
	protected Set<String> areaNames;
	protected String obsFilePath;

	public Generator(String obsFilePath, HashMap<String, String> associations, String urlIn, int scala, String startTime,
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

	protected long timestampsDifference(LocalDateTime previous, LocalDateTime current, int scala) {
		return Math.abs(Duration.between(previous,current).toMillis()/scala);
	}


	protected void sendSample(CSVRecord r){
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
}
