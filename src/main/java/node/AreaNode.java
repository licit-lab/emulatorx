package node;

import link.Link;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;


public abstract class AreaNode {

	private String areaName;
	private HashMap<Long, Link> links= new HashMap<>(); //It maintains the links for each area, the link ID is used as the key
	private ClientConsumer consumer;
	private ClientSession sessionOut;
	private ClientProducer producer;
	private MessageHandler handler;
	private boolean multipleNorthBoundQueues;
	private String urlIn;
	private String urlOut;
	private static final Logger log = LoggerFactory.getLogger(AreaNode.class);
	protected DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

	public AreaNode(String urlIn, String urlOut, String areaName, boolean multipleQueues) {
		this.areaName = areaName;
		this.urlIn = urlIn;
		this.urlOut = urlOut;
		multipleNorthBoundQueues = multipleQueues;
		createConnections();
		setHandler(createMessageHandler());
		setQueueListener();
	}

	public void addLink(Link l) {
		getLinks().put(l.getId(),l);
	}

	public HashMap<Long, Link> getLinks() {
		return links;
	}

	private void setHandler(MessageHandler handler) {
		this.handler = handler;
	}

	private void createConnections() {
		ClientSessionFactory factoryIn;
		ClientSessionFactory factoryOut;
		try {
			ServerLocator locatorIn = ActiveMQClient.createServerLocator(urlIn);
			factoryIn = locatorIn.createSessionFactory();
			ClientSession sessionIn = factoryIn.createSession(true, true);
			sessionIn.createQueue(new SimpleString(areaName), RoutingType.ANYCAST, new SimpleString(areaName), true);
			consumer = sessionIn.createConsumer(areaName);
			sessionIn.start();

			ServerLocator locatorOut = ActiveMQClient.createServerLocator(urlOut);
			factoryOut = locatorOut.createSessionFactory();
			sessionOut = factoryOut.createSession(true,true);
			sessionOut.start();

			if(multipleNorthBoundQueues) {
				String NORTHBOUND_SUFFIX = "-Northbound";
				sessionOut.createQueue(new SimpleString(areaName + NORTHBOUND_SUFFIX), RoutingType.ANYCAST, new SimpleString(areaName + NORTHBOUND_SUFFIX), true);
				producer = sessionOut.createProducer(new SimpleString(areaName + NORTHBOUND_SUFFIX));
			} else {
				sessionOut.createQueue(new SimpleString("NorthBound"), RoutingType.ANYCAST, new SimpleString("NorthBound"), true);
				producer = sessionOut.createProducer(new SimpleString("NorthBound"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

	public void sendMessage(long linkId, String messageBody){
		ClientMessage msg;
		try {
			msg = sessionOut.createMessage(true);
			msg.getBodyBuffer().writeString(messageBody);
			producer.send(msg);
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