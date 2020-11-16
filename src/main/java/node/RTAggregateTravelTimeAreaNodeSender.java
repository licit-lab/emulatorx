package node;

import link.Links;
import link.RTLink;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class RTAggregateTravelTimeAreaNodeSender extends Thread {
	private ClientProducer producer;
	private ClientSession session;
	private Links links;
	private static final Logger log = LoggerFactory.getLogger(RTAggregateTravelTimeAreaNodeSender.class);

	public RTAggregateTravelTimeAreaNodeSender(ClientSession session, ClientProducer producer, Links links){
		this.producer = producer;
		this.session = session;
		this.links = links;
	}

	public RTAggregateTravelTimeAreaNodeSender(){

	}

	@Override
	public void run() {
		log.info("Sending messages northbound...");
		Set<Long> linkIds = links.getLinks().keySet();
		for(Long linkId: linkIds){
			RTLink l = (RTLink) links.getLinks().get(linkId);
			log.info("Sending messages for link {}",linkId);
			try {
				if(l.isChanged())
					sendMessage(l.getAggregateTotalVehiclesTravelTime());;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void sendMessage(String messageBody){
		ClientMessage msg;
		try {
			msg = session.createMessage(true);
			msg.getBodyBuffer().writeString(messageBody);
			producer.send(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
