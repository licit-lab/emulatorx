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
	private HashMap<Long, Link> links= new HashMap<>();
	private ClientConsumer consumer;
	private ClientSession sessionIn;
	private ClientSession sessionOut;
	private ClientProducer producer;
	private AreaNode myInstance = this;
	private MessageHandler myHandler;
	private boolean multipleNorthBoundQueues;
	private String urlIn;
	private String urlOut;
	private static final Logger log = LoggerFactory.getLogger(AreaNode.class);
	protected DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

	private final String NORTHBOUND_SUFFIX = "-NorthBound";

	public AreaNode(String urlIn, String urlOut, String areaName, HashMap<Long, Link> links, boolean multipleQueues) {
		this(urlIn, urlOut,areaName, multipleQueues);
		this.setLinks(links);
	}

	public AreaNode(String urlIn, String urlOut, String areaName, Link link, boolean multipleQueues) {
		this(urlIn, urlOut,areaName, multipleQueues);
		getLinks().put(link.getId(),link);
	}

	private AreaNode(String urlIn, String urlOut, String areaName, boolean multipleQueues) {
		this.areaName = areaName;
		this.urlIn=urlIn;
		this.urlOut=urlOut;
		multipleNorthBoundQueues=multipleQueues;
		createConnections();
		setMyHandler(createMessageHandler());
		setQueueListener();

	}

	public void addLink(Link l) {
		getLinks().put(l.getId(),l);
	}

	public HashMap<Long, Link> getLinks() {
		return links;
	}

	public void setLinks(HashMap<Long, Link> links) {
		this.links = links;
	}

	public AreaNode getAreaNodeInstance() {
		return myInstance;
	}

	private void setMyHandler(MessageHandler myHandler) {
		this.myHandler = myHandler;
	}

	private void createConnections() {
		ClientSessionFactory factoryIn;
		ClientSessionFactory factoryOut;
		try {
			ServerLocator locatorIn = ActiveMQClient.createServerLocator("tcp://localhost:61616");
			factoryIn = locatorIn.createSessionFactory();
			sessionIn = factoryIn.createSession(true,true);
			sessionIn.createQueue(new SimpleString(areaName), RoutingType.ANYCAST, new SimpleString(areaName), true);
			consumer = sessionIn.createConsumer(areaName);
			sessionIn.start();

			ServerLocator locatorOut = ActiveMQClient.createServerLocator(urlOut);
			factoryOut = locatorOut.createSessionFactory();
			sessionOut = factoryOut.createSession(true,true);
			sessionOut.start();

			if(multipleNorthBoundQueues) {
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
	
	public void sendMessage(String messageBody, long linkId, String timestamp, double speed, String travelTime){
		ClientMessage msg;
		try {
			msg = sessionOut.createMessage(true);
			msg.putLongProperty("link", linkId);
			msg.putStringProperty("interval", timestamp);
			msg.putDoubleProperty("avgSpeed", speed);
			msg.putStringProperty("travelTime", travelTime);
			log.info(messageBody);
			msg.getBodyBuffer().writeString(messageBody);
			producer.send(msg);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	public void sendMessageTT(long linkId, String messageBody){
		ClientMessage msg;
		try {
			msg = sessionOut.createMessage(true);
			msg.putLongProperty("link", linkId);
			msg.getBodyBuffer().writeString(messageBody);
			producer.send(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sendMessageTT(String messageBody, long linkId, String timestamp, String traveltime){
		ClientMessage msg;
		try {
			msg = sessionOut.createMessage(true);
			msg.putLongProperty("link", linkId);
			msg.getBodyBuffer().writeString(messageBody);
			producer.send(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void setQueueListener() {
		try {
			consumer.setMessageHandler(myHandler);
		} catch (ActiveMQException e) {
			e.printStackTrace();
		}
	}
	
	protected abstract MessageHandler createMessageHandler() ;
}