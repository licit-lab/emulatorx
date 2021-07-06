package node;

import link.Links;
import link.Link;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.format.DateTimeFormatter;

public abstract class AggregateTravelTimeAreaNode {
	private static final Logger log = LoggerFactory.getLogger(AggregateTravelTimeAreaNode.class);
	protected String areaName;
	protected ClientConsumer consumer;
	protected MessageHandler handler;
	protected String urlIn;
	protected Links links;
	protected int scala;
	protected DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

	public AggregateTravelTimeAreaNode(String urlIn, String areaName, int scala) {
		this.areaName = areaName;
		this.urlIn = urlIn;
		this.links = new Links();
		this.scala = scala;
		createConsumer();
		setHandler(createMessageHandler());
		setQueueListener();
	}

	public void addLink(Link link) {
		this.links.addLink(link);
	}

	private void setHandler(MessageHandler handler) {
		this.handler = handler;
	}

	private void createConsumer() {
		ClientSessionFactory factoryIn;
		try {
			ServerLocator locatorIn = ActiveMQClient.createServerLocator(urlIn);
			factoryIn = locatorIn.createSessionFactory();
			ClientSession sessionIn = factoryIn.createSession(true, true);
			sessionIn.createQueue(new SimpleString(areaName), RoutingType.ANYCAST, new SimpleString(areaName), true);
			consumer = sessionIn.createConsumer(areaName);
			sessionIn.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void setQueueListener() {
		try {
			consumer.setMessageHandler(handler);
		} catch (ActiveMQException e) {
			e.printStackTrace();
		}
	}

	protected abstract MessageHandler createMessageHandler() ;
}
