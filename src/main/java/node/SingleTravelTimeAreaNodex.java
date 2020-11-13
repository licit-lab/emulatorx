package node;

import data.util.PacketGenerator;
import link.Linkx;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleTravelTimeAreaNodex extends AreaNodex {
	private static final Logger log = LoggerFactory.getLogger(SingleTravelTimeAreaNodex.class);

	public SingleTravelTimeAreaNodex(String urlIn, String urlOut, String areaName, boolean multipleQueues, int scala) {
		super(urlIn, urlOut,areaName, multipleQueues, scala);
	}

	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				log.info("A new speed reading is about to be processed...");
				long linkProperty = msg.getLongProperty("linkid");
				Linkx link = super.links.getLink(linkProperty);
				log.info("The speed reading refers to link {}", linkProperty);
				//Computing single vehicle travel time
				double singleVehicleTravelTime = link.computeSingleVehicleTravelTime(msg.getFloatProperty("speed"),msg.getFloatProperty("coverage"));
				log.info("The travelTime computed value is: " + singleVehicleTravelTime);
				//Generating Payload
				String singleVehicleTravelTimePayload = PacketGenerator.singleVehicleTravelTimeSample(linkProperty,singleVehicleTravelTime,msg.getStringProperty("timestamp"));
				log.info("The northbound message payload will be {}", singleVehicleTravelTimePayload);
				/*super.sendMessage(singleVehicleTravelTimePayload);
				log.info("Message has been sent.");*/
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}
}