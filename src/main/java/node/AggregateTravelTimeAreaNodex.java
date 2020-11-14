package node;

import link.Linkx;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Set;

public class AggregateTravelTimeAreaNodex extends AreaNodex {
	private static final Logger log = LoggerFactory.getLogger(AggregateTravelTimeAreaNodex.class);

	public AggregateTravelTimeAreaNodex(String urlIn, String urlOut, String areaName, boolean multipleQueues, int scala) {
		super(urlIn, urlOut, areaName, multipleQueues,scala);
	}

	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				if(msg.getBooleanProperty("placeholder")){
					log.info("It's a placeholder sample, sending the aggregate packet if at least a vehicle has transited");
					Set<Long> keySet = super.links.getLinks().keySet();
					for(Long l: keySet){
						Linkx link = super.links.getLinks().get(l);
						String packet = link.getAggregateTotalVehiclesTravelTime();
						if(packet != null)
							super.sendMessage(packet);
					}
				}
				else{
					long linkId = msg.getLongProperty("linkid");
					log.info("The link is the following {}", linkId);
					Linkx link = super.links.getLink(linkId);
					log.info("A new speed reading is about to be processed... ");
					log.info("The speed reading refers to link {}", linkId);
					log.info("The speed reading timestamp is {}",msg.getStringProperty("timestamp"));
					//Update link
					link.updateAggregateTotalVehiclesTravelTime(LocalDateTime.parse(msg.getStringProperty("timestamp"),formatter),
							msg.getFloatProperty("speed"),msg.getFloatProperty("coverage"));
				}
				msg.individualAcknowledge();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}

}