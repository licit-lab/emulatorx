package link;

import data.util.PacketGenerator;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;

public class Link {
	protected long linkId;
	protected float length;
	protected int ffs,speedlimit,frc,netclass,fow;
	protected String routenumber,areaname,name;
	protected double totalTravelTime;
	protected float[][] geom;
	protected int intervallo;
	protected double totalSampleSpeeds;
	protected int numVehicles;
	protected LocalDateTime startDateTime, endDateTime, currentDateTime;
	protected DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
	protected DescriptiveStatistics stats = null;

	protected static final double FACTOR_M2KM = 0.001;
	protected static final double FACTORH_2SEC = 3600;

	private static final Logger log = LoggerFactory.getLogger(Link.class);

	public Link(long id, float length, int ffs, int speedlimit, int frc, int netclass, int fow, String routenumber,
				String areaname, String name, String geom, int intervallo, String startDateTime) {
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
		setIntervalBounds(startDateTime);
		totalTravelTime = 0;
		getStats();
	}

	protected void getStats(){
		if(this.stats == null)
			this.stats = new DescriptiveStatistics();
	}

	/*
	Set the interval for computing average
	 */
	protected void setIntervalBounds(String startDateTime) {
		this.startDateTime = LocalDateTime.parse(startDateTime,formatter);
		log.info("Starting date is {}", this.startDateTime.toString());
		this.endDateTime = this.startDateTime.plusMinutes(intervallo).minusSeconds(1);
		log.info("Ending date is {}", this.endDateTime.toString());
	}


	protected void updateFinalDate(int mul){
		this.startDateTime = this.startDateTime.plusMinutes(intervallo*mul);
		this.endDateTime = this.startDateTime.plusMinutes(intervallo).minusSeconds(1);
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


	protected void resetAggregateTotalVehiclesTravelTime(){
		numVehicles = 0;
		stats.clear();
	}

	public String computeTotalVehiclesTravelTime(LocalDateTime receivedDate, float sampleSpeed, float coverage) {
		String totalVehiclesTravelTime = null;
		if(receivedDate.isAfter(endDateTime)){
			log.info("The speed reading is outside the interval upper bounds. Creating the packet and resetting the counters...");
			log.info("Number of vehicles transited is {}", numVehicles);
			log.info("Total travel time amounts to {}", totalTravelTime);
			totalVehiclesTravelTime = PacketGenerator.totalVehiclesTravelTimeSample(getId(),totalTravelTime,numVehicles, startDateTime, endDateTime);
			totalSampleSpeeds = 0;
			numVehicles = 0;
			totalTravelTime = 0;
			/*Duration duration =  Duration.between(receivedDate,finalDate);
			log.info("Difference between final date and received date for the current interval {} mins", duration.toMinutes());
			long diff = Math.abs(duration.toMinutes());
			int mul = (int) (diff/intervallo);
			log.info("The multiplier is {}", mul);
			mul++;
			this.finalDate = finalDate.plusMinutes(mul*intervallo);
			this.startingDate = finalDate.minusMinutes(intervallo);*/
			this.startDateTime = receivedDate;
			this.endDateTime = receivedDate.plusMinutes(intervallo);
			log.info("New starting date is {}", startDateTime.toString());
			log.info("New final date is {}", endDateTime.toString());
		}
		numVehicles++;
		totalSampleSpeeds = totalSampleSpeeds + sampleSpeed;
		//Length is converted into km and subsequent travel time in seconds
		//When coverage is zero, the observation is still processed but has no effect
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