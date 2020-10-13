package node;

import link.Link;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.BasicConfigurator;
import util.SettingReader;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Classe utilizzata per l'inizializzazione dei vari AreaNode. Il processo parte dalla lettura da file delle informazioni
 * dei singoli link. Da questi si recuperano il numero di aree e di conseguenza si instanziano i diversi AreaNode, contestualmente vengono generati
 * tutti i link.
 */
public class Initialization extends Thread{

	private enum SensorType {
		SINGLETRAVELTIME,
		TOTALTRAVELTIME
	}

	private static final Logger log = LoggerFactory.getLogger(Initialization.class);

	public void run(){
		BasicConfigurator.configure();
		SensorType sensorType;
		SettingReader st = new SettingReader();
		String value = st.readElementFromFileXml("settings.xml", "areaNode", "sensorType");
		System.out.println(value);
		if (Integer.parseInt(value) == 0 )
			sensorType = SensorType.SINGLETRAVELTIME;
		else
			sensorType = SensorType.TOTALTRAVELTIME;
		log.info("Sensor type is: " + sensorType);

		boolean boolMultipleNorthBoundQueues;
		value = st.readElementFromFileXml("settings.xml", "areaNode", "multipleNorthBoundQueues");
		boolMultipleNorthBoundQueues = Integer.parseInt(value) == 1 ;
		log.info("MultipleNorthBoundQueues has been set to: " + boolMultipleNorthBoundQueues);

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

		HashMap<String, AreaNode> areas = new HashMap<>(); //It maintains associations between links and areas

		Reader reader;
		try {
			reader = Files.newBufferedReader(Paths.get(linkFilePath));
			CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';');
			CSVParser csvParser = csvFormat.parse(reader);
			for (CSVRecord r: csvParser){
				AreaNode an = null;
				Link link = new Link(Long.parseLong(r.get("id")), Float.parseFloat(r.get("length").replace(',','.')),
						Integer.parseInt(r.get("ffs")), Integer.parseInt(r.get("speedlimit")), Integer.parseInt(r.get("frc")),
						Integer.parseInt(r.get("netclass")), Integer.parseInt(r.get("fow")),
						r.get("routenumber"),r.get("areaname"),r.get("name"),r.get("geom"),
						Integer.parseInt(interval),
						startTime,msgType);
				if(areas.containsKey(r.get("areaname")))
					areas.get(r.get("areaname")).addLink(link);
				else {
					if (sensorType == SensorType.SINGLETRAVELTIME){
						an = new SingleTravelTimeAreaNode(urlIn, urlOut, r.get("areaname"), boolMultipleNorthBoundQueues);
						an.addLink(link);
					}
					else if (sensorType == SensorType.TOTALTRAVELTIME){
						an = new TotalTravelTimeAreaNode(urlIn, urlOut, r.get("areaname"), boolMultipleNorthBoundQueues);
						an.addLink(link);
					}
					areas.put(r.get("areaname"), an);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Initialization i = new Initialization();
		i.start();

		try
		{
			i.wait();
		}catch(Exception e) {
			System.out.println(e);
		}
	}
}