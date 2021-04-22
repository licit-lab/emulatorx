package link;

import data.util.PacketGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.LateSample;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.Queue;

public class RTLink extends Link {
	private static final Logger log = LoggerFactory.getLogger(RTLink.class);
	private boolean check = false;
	private Queue<LateSample> lateSamples;

	public RTLink(long id, float length, int ffs, int speedlimit, long from, long to,
				  String areaname, String name, String coordinates, int intervallo, String startDateTime) {
		super(id, length, ffs, speedlimit, from, to, areaname, name, coordinates, intervallo, startDateTime);
		this.lateSamples = new LinkedList<>();
	}

	public synchronized void updateAggregateTotalVehiclesTravelTime(LocalDateTime receivedDateTime, float sampleSpeed, float coverage) throws InterruptedException {
		log.info("Updating aggregated packet in RT solution...");
		this.currentDateTime = receivedDateTime;
		if((currentDateTime.isAfter(startDateTime) && currentDateTime.isBefore(endDateTime)) ||
				currentDateTime.isEqual(startDateTime) || currentDateTime.isEqual(endDateTime)){
			if(lateSamples.size() != 0)
				for(int i = 0; i < lateSamples.size(); i++){
					LateSample ls = lateSamples.remove();
					numVehicles++;
					stats.addValue(((ls.getCoverage()*length*FACTOR_M2KM)/ls.getSampleSpeed())*FACTORH_2SEC);
				}
			numVehicles++;
			assert stats != null;
			stats.addValue(((coverage*length*FACTOR_M2KM)/sampleSpeed)*FACTORH_2SEC);
		} else if(currentDateTime.isBefore(startDateTime)){
			log.warn("Packet arrived too late, so it will dropped");
		} else if(currentDateTime.isAfter(endDateTime)){
			log.warn("Packet arrived too early, so it will be queued");
			LateSample ls = new LateSample(receivedDateTime,sampleSpeed,coverage);
			lateSamples.add(ls);
		}
		/*if(currentDate.isEqual(finalDate) || (currentDate.isAfter(finalDate)
				&& finalDate.isBefore(finalDate.plusSeconds(10)))){
			log.warn("This sample has a timestamp around the interval limit, so I will wait...");
			while(!check) {
				log.warn("Waiting 10 seconds...");
				this.wait(10000);
			}
			check = false;
		}

		notify();*/
	}

	public synchronized String getAggregateTotalVehiclesTravelTime() throws InterruptedException {
		/*log.warn("Giving 10 seconds for late samples to be aggregated....");
		this.wait(10000);*/
		String aggregateVehiclesTravelTime = null;
		if(numVehicles > 0) {
			log.info("Creating the packet and resetting the counters...");
			log.info("Number of vehicles transited is {}", numVehicles);
			double avgTravelTime = stats.getMean();
			double sdTravelTime = stats.getStandardDeviation();
			Duration d = Duration.between(startDateTime, endDateTime);
			aggregateVehiclesTravelTime = PacketGenerator.aggregateVehiclesTravelTimeSample(getId(), avgTravelTime, sdTravelTime, numVehicles,
					d, startDateTime, endDateTime,0);
			resetAggregateTotalVehiclesTravelTime();
		}
		//wake up other thread
		/*log.info("{}",currentDate);
		if(currentDate.isEqual(finalDate) || (currentDate.isAfter(finalDate)
				&& finalDate.isBefore(finalDate.plusSeconds(10)))){
			log.warn("Allowing the processing of the borderline sample...");
			check = true;
		}*/
		/*long diff = Math.abs(Duration.between(currentDate,finalDate).toMinutes());
		int mul = (int) (diff/intervallo);
		log.info("The multiplier is {}", mul);
		mul++;*/
		updateFinalDate(1);
		/*this.notify();*/
		return aggregateVehiclesTravelTime;
	}
}
