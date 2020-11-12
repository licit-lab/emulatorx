package node;

import link.Links;
import link.Linkx;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class AreaNodexSender extends Thread {
	private ClientProducer producer;
	private ClientSession session;
	private Links links;
	private static final Logger log = LoggerFactory.getLogger(AreaNodexSender.class);

	public AreaNodexSender(ClientSession session, ClientProducer producer, Links links){
		this.producer = producer;
		this.session = session;
		this.links = links;
	}

	public AreaNodexSender(){

	}

	@Override
	public void run() {
		log.info("Sending messages northbound...");
		Set<Long> linkIds = links.getLinks().keySet();
		for(Long linkId: linkIds){
			Linkx l = links.getLinks().get(linkId);
			log.info("Sending messages for link {}",linkId);
			String msg = null;
			try {
				msg = l.getAggregateTotalVehiclesTravelTime();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			log.info("MSG = {}", msg);
			if(msg != null)
				sendMessage(msg);
			else
				log.info("No message has been sent");
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
