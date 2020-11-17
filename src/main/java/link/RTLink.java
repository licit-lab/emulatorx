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

	public RTLink(long id, float length, int ffs, int speedlimit, int frc, int netclass, int fow, String routenumber, String areaname, String name, String geom, int intervallo, String startingDate) {
		super(id, length, ffs, speedlimit, frc, netclass, fow, routenumber, areaname, name, geom, intervallo, startingDate);
		this.lateSamples = new LinkedList<>();
	}

	public synchronized void updateAggregateTotalVehiclesTravelTime(LocalDateTime receivedDate, float sampleSpeed, float coverage) throws InterruptedException {
		log.info("Updating aggregated packet in RT solution...");
		this.currentDate = receivedDate;
		if(currentDate.isAfter(startingDate) && currentDate.isBefore(finalDate)){
			if(lateSamples.size() != 0)
				for(int i = 0; i < lateSamples.size(); i++){
					LateSample ls = lateSamples.remove();
					numVehicles++;
					stats.addValue(((ls.getCoverage()*length*FACTOR_M2KM)/ls.getSampleSpeed())*FACTORH_2SEC);
				}
			numVehicles++;
			assert stats != null;
			stats.addValue(((coverage*length*FACTOR_M2KM)/sampleSpeed)*FACTORH_2SEC);
		} else if(currentDate.isBefore(startingDate)){
			log.warn("Packet arrived too late, so it will dropped");
		} else if(currentDate.isAfter(finalDate)){
			log.warn("Packet arrived too early, so it will be queued");
			LateSample ls = new LateSample(receivedDate,sampleSpeed,coverage);
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
			Duration d = Duration.between(startingDate, finalDate);
			aggregateVehiclesTravelTime = PacketGenerator.aggregateVehiclesTravelTimeSample(getId(), avgTravelTime, sdTravelTime, numVehicles,
					d, startingDate, finalDate);
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
