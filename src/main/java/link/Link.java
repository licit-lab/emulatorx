package link;

import com.google.gson.Gson;
import data.model.CompleteMessagge;
import data.model.SyntheticMessage;
import data.util.PacketGenerator;
import node.AreaNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Calendar;

public class Link {
	private long linkId;
	private float length;
	private int ffs,speedlimit,frc,netclass,fow;
	private String routenumber,areaname,name;
	private double totalTravelTime;
	private float[][] geom;
	private int intervallo;
	private double totalSampleSpeeds;
	private int numVehicles;
	private LocalDateTime startingDate, finalDate;
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

	private static final double FACTOR_M2KM = 0.001;
	private static final double FACTORH_2SEC = 3600;

	private static final Logger log = LoggerFactory.getLogger(AreaNode.class);

	public Link(long id, float length, int ffs, int speedlimit, int frc, int netclass, int fow, String routenumber,
				String areaname, String name, String geom, int intervallo, String startingDate) {
		this.linkId = id;
		this.length = length;
		this.ffs = ffs;
		this.speedlimit = speedlimit;
		this.frc = frc;
		this.netclass = netclass;
		this.fow = fow;
		this.routenumber = routenumber;
		this.areaname = areaname;
		this.name = name;
		setGeomFromString(geom);
		this.intervallo = intervallo;
		totalSampleSpeeds = 0;
		numVehicles = 0;
		setIntervalBounds(startingDate);
		totalTravelTime = 0;
	}

	/*
	Set the interval for computing average
	 */
	private void setIntervalBounds(String startingDate) {
		this.startingDate = LocalDateTime.parse(startingDate,formatter);
		this.finalDate = this.startingDate.plusMinutes(intervallo);
	}

	private void setGeomFromString(String s) {
		String[] l = s.split("\\|");
		geom = new float[l.length][2];
		for (int i = 0; i < l.length; i++) {
			String[] n = l[i].split(",");
			for (int j = 0; j < n.length; j++)
				geom[i][j] = Float.parseFloat(n[j]);
		}
	}

	public String computeTotalVehiclesTravelTime(LocalDateTime receivedDate, float sampleSpeed, float coverage) {
		String totalVehiclesTravelTime = null;
		if(receivedDate.isAfter(finalDate)){
			log.info("The speed reading is outside the interval upper bounds. Creating the packet and resetting the counters...");
			totalVehiclesTravelTime = PacketGenerator.totalVehiclesTravelTimePayload(getId(),totalTravelTime,numVehicles,startingDate,finalDate);
			totalSampleSpeeds = 0;
			numVehicles = 0;
			totalTravelTime = 0;
			Duration duration =  Duration.between(receivedDate,finalDate);
			log.info("Difference between final date and received date for the current interval {} mins", duration.toMinutes());
			long diff = Math.abs(duration.toMinutes());
			int mul = (int) (diff/intervallo);
			log.info("The multiplier is {}", mul);
			mul++;
			this.finalDate = finalDate.plusMinutes(mul*intervallo);
			this.startingDate = finalDate.minusMinutes(intervallo);
		}
		numVehicles++;
		totalSampleSpeeds = totalSampleSpeeds + sampleSpeed;
		totalTravelTime = totalTravelTime + ((coverage*length*FACTOR_M2KM)/sampleSpeed)*FACTORH_2SEC;
		return totalVehiclesTravelTime;
	}

	public double computeSingleVehicleTravelTime (float sampleSpeed, float coverage) {
		return (coverage*length*FACTOR_M2KM/sampleSpeed)*FACTORH_2SEC;
	}

	//converte le ore trascorse in un oggetto date in una stringa
	/**
	 * in alcune funzioni devo restituire il tempo di percorrenza.
	 * dato che la variabile travelTime  un numero decimale che rappresenta le ore
	 * e voglio restituire una stringa in formato HH:mm:ss per renderla pi comprensibile
	 * viene usata questa funzione per fare la conversione
	 * 
	 * @param hoursToConvert le ore da convertire nel formato HH:mm:ss
	 * @return le ore convertite nel formato HH:mm:ss
	 */
	private String convertHoursInStringDate(double hoursToConvert) {
		Calendar c = Calendar.getInstance();
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		c.add(Calendar.SECOND, (int) (hoursToConvert*3600));
		DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		return dateFormat.format(c.getTime());
	}

	public long getId() {
		return linkId;
	}

	/*public String generateCompleteJsonBody(double avgSpeed, double avgTravelTime, String timestamp) {
		CompleteMessagge cm = new CompleteMessagge(linkId, length,ffs,speedlimit,frc,netclass,fow,routenumber,
				areaname,name,Arrays.deepToString(geom),timestamp,avgSpeed,convertHoursInStringDate(avgTravelTime));
		return new Gson().toJson(cm);
	}

	public String generateSyntheticJsonBody(double avgSpeed, double avgTravelTime, String timestamp) {
		SyntheticMessage sm = new SyntheticMessage(linkId, areaname,timestamp,avgSpeed,convertHoursInStringDate(avgTravelTime));
		return new Gson().toJson(sm);
	}

	*//*public String generateMessageAvgTravelTime(double avgTravelTime , String timestamp) throws ParseException
	{
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

		Date endTime=dateFormat.parse(timestamp);
		Calendar cal;
		cal= Calendar.getInstance();

		cal.setTime(endTime);
		cal.add(Calendar.MINUTE, -3);
		Date startTime=cal.getTime();

		TotalVehiclesTravelTimePayload payload=new TotalVehiclesTravelTimePayload("TotalVehiclesTravelTimePayload", avgTravelTime, 0, startTime, endTime);
		ArrayList<Payload> list= new ArrayList<>();
		list.add(payload);
		Packet p = new Packet(this.linkId, list);
		return new Gson().toJson(p);
	}*/
}