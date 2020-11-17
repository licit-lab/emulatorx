package node;

import link.STLink;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Set;

public class STAggregateTravelTimeAreaNode extends AggregateTravelTimeAreaNodex {
	private static final Logger log = LoggerFactory.getLogger(STAggregateTravelTimeAreaNode.class);

	public STAggregateTravelTimeAreaNode(String urlIn, String urlOut, String areaName, boolean multipleQueues, int scala) {
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				log.info("STAggregateTravelTimeAreaNode is handling another sample...");
				if(msg.getBooleanProperty("placeholder")){
					log.info("It's a placeholder sample, sending the aggregate packet if at least a vehicle has transited");
					Set<Long> keySet = super.links.getLinks().keySet();
					for(Long l: keySet) {
						STLink link = (STLink) super.links.getLinks().get(l);
						String packet = link.getAggregateTotalVehiclesTravelTime();
						if (packet != null)
							sendMessage(packet);
					}
				}
				else{
					long linkId = msg.getLongProperty("linkid");
					log.info("The link is the following {}", linkId);
					STLink link = (STLink) super.links.getLink(linkId);
					log.info("A new speed reading is about to be processed... ");
					log.info("The speed reading refers to link {}", linkId);
					log.info("The speed reading timestamp is {}",msg.getStringProperty("timestamp"));
					//Update link
					if(msg.getFloatProperty("coverage") != 0)
						link.updateAggregateTotalVehiclesTravelTime(LocalDateTime.parse(msg.getStringProperty("timestamp"),formatter),
								msg.getFloatProperty("speed"),msg.getFloatProperty("coverage"));
				}
				msg.individualAcknowledge();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}

	public void sendMessage(String messageBody){
		ClientMessage msg;
		try {
			msg = sessionOut.createMessage(true);
			msg.getBodyBuffer().writeString(messageBody);
			producer.send(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}