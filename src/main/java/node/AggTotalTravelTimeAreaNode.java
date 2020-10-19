package node;

import link.Link;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.geom.Area;
import java.time.LocalDateTime;

public class AggTotalTravelTimeAreaNode extends AreaNode {
	private static final Logger log = LoggerFactory.getLogger(AreaNode.class);

	public AggTotalTravelTimeAreaNode(String urlIn, String urlOut, String areaName, boolean multipleQueues) {
		super(urlIn, urlOut, areaName, multipleQueues);
	}

	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				log.info("A new speed reading is about to be processed... ");
				long linkProperty = msg.getLongProperty("linkid");
				log.info("The link is the following {}", linkProperty);
				Link link = getLinks().get(linkProperty);
				log.info("The speed reading refers to link {}", linkProperty);
				//Computing total travel times
				String aggTotalVehiclesTravelTime = link.computeAggTotalVehiclesTravelTime(LocalDateTime.parse(msg.getStringProperty("timestamp"),formatter),
						msg.getFloatProperty("speed"),msg.getFloatProperty("coverage"));
				if(aggTotalVehiclesTravelTime != null){
					log.info("The northbound message payload will be {}", aggTotalVehiclesTravelTime);
					super.sendMessage(linkProperty,aggTotalVehiclesTravelTime);
					log.info("Message has been sent.");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}

}