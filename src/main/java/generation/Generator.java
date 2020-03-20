package generation;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.SettingReader;
import java.util.HashMap;

import static generation.GeneratorUtils.*;

public class Generator {
	private static int scala = 10;
	private static HashMap<String, String> associations = new HashMap<>();
	private static final Logger log = LoggerFactory.getLogger(Generator.class);

	public static void main(String[] args)throws InterruptedException {
		BasicConfigurator.configure();
		log.info("Launching the generator...");

		SettingReader st = new SettingReader();
		String value = st.readElementFromFileXml("settings.xml", "generator", "scala");
		scala = Integer.parseInt(value);
		log.info("Scala value has been set at: " + scala);

		ClientSessionFactory factory;
		ClientSession session = null;
		try {
			ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616");
			factory = locator.createSessionFactory();
			session = factory.createSession();
		} catch (Exception e) {
			log.error(e.getMessage());
		}

		associateLink2Area(session);
		sendMessage(session);
	}

	private static void associateLink2Area(ClientSession session) {
		SettingReader st = new SettingReader();
		String linksFilePath = st.readElementFromFileXml("settings.xml", "Files", "links");
		associate(session, linksFilePath, associations);
	}

	private static void sendMessage(ClientSession session) throws InterruptedException {
		SettingReader st = new SettingReader();
		String obsFilePath = st.readElementFromFileXml("settings.xml", "Files", "observations");
		createAndSend(session, obsFilePath, scala, associations);
	}
}