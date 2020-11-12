package node;

import link.Linkx;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class AggregateTravelTimeAreaNodex extends AreaNodex {
	private static final Logger log = LoggerFactory.getLogger(AggregateTravelTimeAreaNodex.class);

	public AggregateTravelTimeAreaNodex(String urlIn, String urlOut, String areaName, boolean multipleQueues) {
		super(urlIn, urlOut, areaName, multipleQueues);
	}

	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				log.info("A new speed reading is about to be processed... ");
				long linkId = msg.getLongProperty("linkid");
				log.info("The link is the following {}", linkId);
				Linkx link = super.links.getLink(linkId);
				log.info("The speed reading refers to link {}", linkId);
				log.info("The speed reading timestamp is {}",msg.getStringProperty("timestamp"));
				//Update link
				link.updateAggregateTotalVehiclesTravelTime(LocalDateTime.parse(msg.getStringProperty("timestamp"),formatter),
						msg.getFloatProperty("speed"),msg.getFloatProperty("coverage"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}

}