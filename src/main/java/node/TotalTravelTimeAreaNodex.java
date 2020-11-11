package node;

import link.Link;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;

public class TotalTravelTimeAreaNodex extends AreaNodex {
	private static final Logger log = LoggerFactory.getLogger(TotalTravelTimeAreaNodex.class);

	public TotalTravelTimeAreaNodex(String urlIn, String urlOut, String areaName, boolean multipleQueues) {
		super(urlIn, urlOut, areaName, multipleQueues);
	}

	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				log.info("A new speed reading is about to be processed... ");
				long linkProperty = msg.getLongProperty("linkid");
				log.info("The link is the following {}", linkProperty);
				Link link = super.links.getLink(linkProperty);
				log.info("The speed reading refers to link {}", linkProperty);
				//Computing total travel times
				String totalVehiclesTravelTime = link.computeTotalVehiclesTravelTime(LocalDateTime.parse(msg.getStringProperty("timestamp"),formatter),
						msg.getFloatProperty("speed"),msg.getFloatProperty("coverage"));
				if(totalVehiclesTravelTime != null){
					/*log.info("The northbound message payload will be {}", totalVehiclesTravelTime);
					super.sendMessage(totalVehiclesTravelTime);
					log.info("Message has been sent.");*/
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}

}