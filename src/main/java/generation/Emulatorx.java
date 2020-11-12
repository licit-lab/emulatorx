package generation;

import link.Linkx;
import node.*;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.SettingReader;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Set;

public class Emulatorx {

	public enum SensorType {
		SINGLETRAVELTIME,
		TOTALTRAVELTIME,
		AGGTOTALTRAVELTIME
	}

	private static final Logger log = LoggerFactory.getLogger(Emulatorx.class);
	private static HashMap<String, String> associations = new HashMap<>();

	public static void main(String[] args) {
		SensorType sensorType = null;
		SettingReader st = new SettingReader();
		String value = st.readElementFromFileXml("settings.xml", "areaNode", "sensorType");
		System.out.println(value);
		if (Integer.parseInt(value) == 0 )
			sensorType = SensorType.SINGLETRAVELTIME;
		else if (Integer.parseInt(value) == 1)
			sensorType = SensorType.TOTALTRAVELTIME;
		else if (Integer.parseInt(value) == 2)
			sensorType = SensorType.AGGTOTALTRAVELTIME;
		log.info("Sensor type is: " + sensorType);

		boolean boolMultipleNorthboundQueues;
		value = st.readElementFromFileXml("settings.xml", "areaNode", "multipleNorthboundQueues");
		boolMultipleNorthboundQueues = Integer.parseInt(value) == 1 ;
		log.info("MultipleNorthboundQueues has been set to: " + boolMultipleNorthboundQueues);

		String urlIn = st.readElementFromFileXml("settings.xml", "areaNode", "urlIn");
		String urlOut = st.readElementFromFileXml("settings.xml", "areaNode", "urlOut");
		log.info("Broker in: " + urlIn);
		log.info("Broker out: " + urlOut);

		String interval = st.readElementFromFileXml("settings.xml", "Link", "intervallo");
		String startTime = st.readElementFromFileXml("settings.xml", "Link", "startTime");
		log.info("Interval is set at: " + interval);
		log.info("StartTime is set at: " + startTime);

		value = st.readElementFromFileXml("settings.xml", "Link", "msgType");
		int msgType = Integer.parseInt(value);
		log.info("Message type is set at: " + msgType);

		String linkFilePath = st.readElementFromFileXml("settings.xml", "Files", "links");
		log.info("Links file path is: " + linkFilePath);

		value = st.readElementFromFileXml("settings.xml", "generator", "scala");
		int scala = Integer.parseInt(value);
		log.info("Scala value has been set at: " + scala);

		HashMap<String, AreaNodex> areas = new HashMap<>(); //It maintains associations between links and areas

		ClientSessionFactory factory;
		ClientSession session = null;
		try {
			ServerLocator locator = ActiveMQClient.createServerLocator(urlIn);
			factory = locator.createSessionFactory();
			session = factory.createSession();
		} catch (Exception e) {
			log.error(e.getMessage());
		}

		Reader reader;
		try {
			reader = Files.newBufferedReader(Paths.get(linkFilePath));
			CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';');
			CSVParser csvParser = csvFormat.parse(reader);
			for (CSVRecord r: csvParser){
				//Creating area node and associating links
				AreaNodex an = null;
				Linkx link = new Linkx(Long.parseLong(r.get("id")), Float.parseFloat(r.get("length").replace(',','.')),
						Integer.parseInt(r.get("ffs")), Integer.parseInt(r.get("speedlimit")), Integer.parseInt(r.get("frc")),
						Integer.parseInt(r.get("netclass")), Integer.parseInt(r.get("fow")),
						r.get("routenumber"),r.get("areaname"),r.get("name"),r.get("geom"),
						Integer.parseInt(interval),
						startTime);

				if(areas.containsKey(r.get("areaname")))
					areas.get(r.get("areaname")).addLink(link);
				else {
					if (sensorType == SensorType.SINGLETRAVELTIME){
						an = new SingleTravelTimeAreaNodex(urlIn, urlOut, r.get("areaname"), boolMultipleNorthboundQueues);
						an.addLink(link);
					}
					else if (sensorType == SensorType.TOTALTRAVELTIME){
						an = new TotalTravelTimeAreaNodex(urlIn, urlOut, r.get("areaname"), boolMultipleNorthboundQueues);
						an.addLink(link);
					}
					else if (sensorType == SensorType.AGGTOTALTRAVELTIME){
						an = new AggregateTravelTimeAreaNodex(urlIn, urlOut, r.get("areaname"), boolMultipleNorthboundQueues);
						an.addLink(link);
					}
					areas.put(r.get("areaname"), an);
				}
				//Create associations link -> areaNode
				associations.put(r.get("id"),r.get("areaname"));
				log.info("Link {} is associated to {}", r.get("id"),r.get("areaname"));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			Thread.sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		log.info("Creating the producers...");
		Set<String> areaNames = areas.keySet();
		for(String a: areaNames)
			areas.get(a).createProducer();
		//Then follows the generator
		Generatorx gx = new Generatorx(associations,session,scala,startTime);
		gx.start();
	}
}
