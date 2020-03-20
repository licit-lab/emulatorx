package node;

import com.google.gson.Gson;
import data.model.Packet;
import data.model.Payload;
import data.model.singlevehicle.SingleVehicleTravelTimePayload;
import data.util.PacketGenerator;
import link.Link;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;

public class AvgSpeedVehicleAreaNode extends AreaNode {
	private static final Logger log = LoggerFactory.getLogger(AreaNode.class);

	public AvgSpeedVehicleAreaNode(String urlIn, String urlOut, String areaName, HashMap<Long, Link> links, boolean multipleQueues) {
		super(urlIn, urlOut,areaName,links, multipleQueues);
	}

	public AvgSpeedVehicleAreaNode(String urlIn, String urlOut, String areaName, Link link, boolean multipleQueues) {
		super(urlIn, urlOut,areaName,link, multipleQueues);

	}
	
	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				log.info("A new message is about to be processed...");
				long linkValue = msg.getLongProperty("link");
				Link link = super.getLinks().get(linkValue);
				log.info("The messagge refers to the link: " + linkValue);
				double singleVehicleTravelTime = link.computeSingleVehicleTravelTime(msg.getFloatProperty("speed"),msg.getFloatProperty("coverage"));
				log.info("The travelTime computed value is: " + singleVehicleTravelTime);
				String singleVehicleTravelTimePayload = PacketGenerator.singleVehicleTravelTimePayload(linkValue,singleVehicleTravelTime,msg.getStringProperty("timestamp"));
				log.info("The northbound message payload will be: " + singleVehicleTravelTimePayload);
				super.sendMessageTT(linkValue,singleVehicleTravelTimePayload);
				log.info("Messaggio inviato...");
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}
}