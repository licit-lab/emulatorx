package node;

import data.util.PacketGenerator;
import link.Link;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleTravelTimeAreaNode extends AreaNode {
	private static final Logger log = LoggerFactory.getLogger(AreaNode.class);

	public SingleTravelTimeAreaNode(String urlIn, String urlOut, String areaName, boolean multipleQueues) {
		super(urlIn, urlOut,areaName, multipleQueues);
	}
	
	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				log.info("A new speed reading is about to be processed...");
				long linkProperty = msg.getLongProperty("linkid");
				Link link = super.getLinks().get(linkProperty);
				log.info("The speed reading refers to link {}", linkProperty);
				//Computing single vehicle travel time
				double singleVehicleTravelTime = link.computeSingleVehicleTravelTime(msg.getFloatProperty("speed"),msg.getFloatProperty("coverage"));
				log.info("The travelTime computed value is: " + singleVehicleTravelTime);
				//Generating Payload
				String singleVehicleTravelTimePayload = PacketGenerator.singleVehicleTravelTimePayload(linkProperty,singleVehicleTravelTime,msg.getStringProperty("timestamp"));
				log.info("The northbound message payload will be {}", singleVehicleTravelTimePayload);
				super.sendMessage(linkProperty,singleVehicleTravelTimePayload);
				log.info("Message has been sent.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}
}