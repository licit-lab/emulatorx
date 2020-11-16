package node;

import link.Linkx;
import link.RTLink;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RTAggregateTravelTimeAreaNode extends AggregateTravelTimeAreaNodex {
	private static final Logger log = LoggerFactory.getLogger(RTAggregateTravelTimeAreaNode.class);

	public RTAggregateTravelTimeAreaNode(String urlIn, String urlOut, String areaName, boolean multipleQueues, int scala) {
		super(urlIn, urlOut, areaName, multipleQueues, scala);
	}

	public void createProducer(){
		ClientSessionFactory factoryOut;
		try{
			ServerLocator locatorOut = ActiveMQClient.createServerLocator(urlOut);
			factoryOut = locatorOut.createSessionFactory();
			sessionOut = factoryOut.createSession(true,true);
			sessionOut.start();

			if(multipleNorthboundQueues) {
				String NORTHBOUND_SUFFIX = "-Northbound";
				sessionOut.createQueue(new SimpleString(areaName + NORTHBOUND_SUFFIX), RoutingType.ANYCAST, new SimpleString(areaName + NORTHBOUND_SUFFIX), true);
				producer = sessionOut.createProducer(new SimpleString(areaName + NORTHBOUND_SUFFIX));
			} else {
				sessionOut.createQueue(new SimpleString("Northbound"), RoutingType.ANYCAST, new SimpleString("Northbound"), true);
				producer = sessionOut.createProducer(new SimpleString("Northbound"));
			}
			RTAggregateTravelTimeAreaNodeSender sender = new RTAggregateTravelTimeAreaNodeSender(sessionOut, producer,links);
			ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
			long period = 3/scala;
			executorService.scheduleAtFixedRate(sender,period,period, TimeUnit.MINUTES);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void addLink(Linkx link) {
		this.links.addLink(link);
	}


	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				log.info("RTAggregateTravelTimeAreaNode is handling another sample...");
				long linkId = msg.getLongProperty("linkid");
				log.info("The link is the following {}", linkId);
				RTLink link = (RTLink) super.links.getLink(linkId);
				log.info("A new speed reading is about to be processed... ");
				log.info("The speed reading refers to link {}", linkId);
				log.info("The speed reading timestamp is {}",msg.getStringProperty("timestamp"));
				//Update link
				link.updateAggregateTotalVehiclesTravelTime(LocalDateTime.parse(msg.getStringProperty("timestamp"),formatter),
						msg.getFloatProperty("speed"),msg.getFloatProperty("coverage"));
				msg.individualAcknowledge();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}
}
