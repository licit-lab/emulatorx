package node;

import link.Link;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class AggregateTravelTimeAreaNode extends AreaNode {
	private static final Logger log = LoggerFactory.getLogger(AreaNode.class);

	public AggregateTravelTimeAreaNode(String urlIn, String urlOut, String areaName, boolean multipleQueues) {
		super(urlIn, urlOut, areaName, multipleQueues);
	}

	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				log.info("A new speed reading is about to be processed... ");
				long linkId = msg.getLongProperty("linkid");
				log.info("The link is the following {}", linkId);
				Link link = super.links.getLink(linkId);
				log.info("The speed reading refers to link {}", linkId);
				log.info("The speed reading timestamp is {}",msg.getStringProperty("timestamp"));
				//Computing total travel times
				String aggregateVehiclesTravelTime = link.computeAggTotalVehiclesTravelTime(LocalDateTime.parse(msg.getStringProperty("timestamp"),formatter),
						msg.getFloatProperty("speed"),msg.getFloatProperty("coverage"));
				if(aggregateVehiclesTravelTime != null){
					log.info("The northbound message payload will be {}", aggregateVehiclesTravelTime);
					super.sendMessage(aggregateVehiclesTravelTime);
					log.info("Message has been sent.");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}

}