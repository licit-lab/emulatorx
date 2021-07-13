package node;

import com.google.gson.Gson;
import link.STLink;
import model.AggregateVehiclesTravelTimeSample;
import org.apache.activemq.artemis.api.core.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Set;

public class STAggregateTravelTimeAreaNode extends AggregateTravelTimeAreaNode {
	private static final Logger log = LoggerFactory.getLogger(STAggregateTravelTimeAreaNode.class);
	BufferedWriter writer;


	public STAggregateTravelTimeAreaNode(String urlIn, String areaName,int scala)
			throws IOException {
		super(urlIn, areaName,scala);
		String fileName = areaName+".txt";
		writer = Files.newBufferedWriter(Paths.get(fileName));
	}


	@Override
	protected MessageHandler createMessageHandler() {
		return msg -> {
			try {
				log.info("STAggregateTravelTimeAreaNode is handling another sample...");
				if(msg.getBooleanProperty("placeholder")){
					log.info("It's a placeholder sample, sending the aggregate packet if at least a vehicle has transited");
					Set<String> keySet = super.links.getLinks().keySet();
					int size = 0;
					for(String l: keySet) {
						STLink link = (STLink) super.links.getLinks().get(l);
						if(link.getNumVehicles() > 0)
							size++;
					}
					writer.write(String.valueOf(size));
					writer.newLine();
					for(String l: keySet) {
						STLink link = (STLink) super.links.getLinks().get(l);
						String line = link.getAggregateTotalVehiclesTravelTime();
						if(line != null){
//							System.out.println(line);
//							Gson g = new Gson();
//							AggregateVehiclesTravelTimeSample s = g.fromJson(line, AggregateVehiclesTravelTimeSample.class);
//							s.setAggTimestamp(System.currentTimeMillis());
//							System.out.println(new Gson().toJson(s));
							writer.write(line);
							writer.newLine();
						}
					}
					writer.flush();
				}
				else{
					String linkId = msg.getStringProperty("linkid");
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
}