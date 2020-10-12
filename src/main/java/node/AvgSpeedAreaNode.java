package node;

import link.Link;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.util.HashMap;

public class AvgSpeedAreaNode extends AreaNode {
	private static final Logger log = LoggerFactory.getLogger(AreaNode.class);

	public AvgSpeedAreaNode(String urlIn, String urlOut, String areaName, boolean multipleQueues) {
		super(urlIn, urlOut, areaName, multipleQueues);
	}

	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				long linkValue = msg.getLongProperty("link");
				Link link = getLinks().get(linkValue);
				String totalVehiclesTravelTime = link.computeTotalVehiclesTravelTime(LocalDateTime.parse(msg.getStringProperty("timeStamp"),formatter),
						msg.getFloatProperty("speed"),msg.getFloatProperty("coverage"));
				if(totalVehiclesTravelTime != null){
					super.sendMessageTT(linkValue,totalVehiclesTravelTime);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}

}