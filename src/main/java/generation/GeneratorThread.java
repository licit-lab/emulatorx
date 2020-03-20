package generation;

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.SettingReader;

import java.text.ParseException;
import java.util.HashMap;

import static generation.GeneratorUtils.*;

//TODO Complete multi-threaded version of Generator

/**
 * La classe GeneratorThread.java ha il compito di associare il link alla giusta area e di inviare
 * i sample al corretto nodo area in base al link specificato nel sample. Per l'associazione
 * tra link ed area si sfruttano le informazioni presenti all'interno del file "Links.csv" o di un suo sottoinsieme mentre il 
 * file contenete i samples � "Observation.csv" o di un suo sottoinsieme.
 * @author De Luca Lucio, Grimaldi Gaia, Tedesco Francesco
 *
 */
public class GeneratorThread extends Thread {
	private static int scala=10;
	private String url;
	private static HashMap<String, String> associations=new HashMap<>();
	private String linkFilePath;
	private String obsFilePath;
	private static final Logger log = LoggerFactory.getLogger(Generator.class);

	public GeneratorThread(String linkFile, String obsFile) {
		this.linkFilePath=linkFile;
		this.obsFilePath=obsFile;
	}

	/**
	 * Il metodo run contiene le operazioni eseguite dal thread:
	 * Legge la scala da voler utilizzare e crea una sessione con la quale creer� tante code quante sono le aree presenti nel suo 
	 * file linkFile.
	 * Esegue il riempimento delle code in base a i sample letti nel file con nome presente nella variabile obsFile
	 */
	public void run() {
		SettingReader st = new SettingReader();
		String value = st.readElementFromFileXml("settings.xml", "generator", "scala");
		scala = Integer.parseInt(value);
		log.info("Scala has been set at: " + scala);
		url = st.readElementFromFileXml("settings.xml", "areaNode", "urlIn");
		ClientSessionFactory factory;
		ClientSession session = null;
		try {
			ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:8161");
			factory = locator.createSessionFactory();
			session = factory.createSession();
		} catch (Exception e) {
			e.printStackTrace();
		}

		associateLink2Area(session);
		try {
			sendMessage(session);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static void sendMessage(ClientSession session) throws InterruptedException {
		SettingReader st = new SettingReader();
		String obsFilePath = st.readElementFromFileXml("settings.xml", "Files", "observations");
		createAndSend(session, obsFilePath, scala, associations);
	}


	private void associateLink2Area(ClientSession session) {
		associate(session, linkFilePath, associations);
	}

	/**
	 * Main il cui compito eseguire un numero di thread par al valore presente in setting.xml.
	 * Ad ogni thread passa due stringhe che rappresentano i nomi del file da leggere.
	 * I nomi partono da 0 a numero di thread-1 e devono essere inseriti in due cartelle, link e obs. 
	 * @param args
	 */
	public static void main(String[] args) {
		BasicConfigurator.configure();
		SettingReader st = new SettingReader();
		// righe di codice che servono per cambiare la variabile scala in base al valore presente nel file xml "settings.xml"
		String value = st.readElementFromFileXml("settings.xml", "generator", "numberOfThreads");
		int threads = Integer.parseInt(value);
		System.out.println("threads = " + threads);
		GeneratorThread[] generators = new GeneratorThread[threads];
		for(int i = 0; i < threads; i++)
			generators[i] = new GeneratorThread("links/"+i+".csv","obs/"+i+".csv");
		for(int i = 0; i < threads; i++)
			generators[i].start();
	}
}
