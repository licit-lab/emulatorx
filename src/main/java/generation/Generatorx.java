package generation;

import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.SettingReader;

import java.util.HashMap;

import static generation.GeneratorxUtils.createAndSend;

public class Generatorx extends Thread {
	private static int scala = 10;
	private  static HashMap<String, String> associations; //It maintains associations between links and areas
	private static final Logger log = LoggerFactory.getLogger(Generator.class);
	private  ClientSession session;

	public Generatorx(HashMap<String, String> associations, ClientSession session){
		this.associations = associations;
		this.session = session;
	}

	@Override
	public void run() {
		BasicConfigurator.configure();
		log.info("Launching the generator...");

		SettingReader st = new SettingReader();
		String value = st.readElementFromFileXml("settings.xml", "generator", "scala");
		scala = Integer.parseInt(value);
		log.info("Scala value has been set at: " + scala);

		String urlIn = st.readElementFromFileXml("settings.xml", "areaNode", "urlIn");
		log.info("Broker in: " + urlIn);



		log.info("Sending messages...");
		try {
			sendMessage(session);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static void sendMessage(ClientSession session) throws InterruptedException {
		log.info("Sending messages...");
		SettingReader st = new SettingReader();
		String obsFilePath = st.readElementFromFileXml("settings.xml", "Files", "observations");
		createAndSend(session, obsFilePath, scala, associations);
	}
}
