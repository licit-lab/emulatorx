package link;

import data.util.PacketGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;

public class STLink extends Linkx {
	private static final Logger log = LoggerFactory.getLogger(STLink.class);

	public STLink(long id, float length, int ffs, int speedlimit, int frc, int netclass, int fow, String routenumber, String areaname, String name, String geom, int intervallo, String startingDate) {
		super(id, length, ffs, speedlimit, frc, netclass, fow, routenumber, areaname, name, geom, intervallo, startingDate);
	}

	public synchronized void updateAggregateTotalVehiclesTravelTime(LocalDateTime receivedDate, float sampleSpeed, float coverage) throws InterruptedException {
		log.info("Updating aggregated packet in ST solution...");
		this.currentDate = receivedDate;
		numVehicles++;
		assert stats != null;
		stats.addValue(((coverage*length*FACTOR_M2KM)/sampleSpeed)*FACTORH_2SEC);
	}

	public synchronized String getAggregateTotalVehiclesTravelTime() {
		String aggregateVehiclesTravelTime = null;
		if(numVehicles > 0) {
			log.info("Creating the packet and resetting the counters...");
			log.info("Number of vehicles transited is {}", numVehicles);
			double avgTravelTime = stats.getMean();
			double sdTravelTime = stats.getStandardDeviation();
			Duration d = Duration.between(startingDate,finalDate);
			aggregateVehiclesTravelTime = PacketGenerator.aggregateVehiclesTravelTimeSample(getId(), avgTravelTime, sdTravelTime, numVehicles,
					d, startingDate, finalDate);
			resetAggregateTotalVehiclesTravelTime();
		}
		long diff = Math.abs(Duration.between(currentDate,finalDate).toMinutes());
		int mul = (int) (diff/intervallo);
		log.info("The multiplier is {}", mul);
		mul++;
		updateFinalDate(mul);
		return aggregateVehiclesTravelTime;
	}

	public boolean isChanged(){
		return numVehicles != 0;
	}
}