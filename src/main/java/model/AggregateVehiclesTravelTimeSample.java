package model;

import com.google.gson.Gson;

public class AggregateVehiclesTravelTimeSample{
	private double avgTravelTime;
	private double sdTravelTime;
	private  int numVehicles;
	private long aggPeriod, domainAggTimestamp, aggTimestamp;
	private String linkid, areaName;

	public AggregateVehiclesTravelTimeSample(String linkid, String areaName, double avgTravelTime, double sdTravelTime,
                                             int numVehicles,
                                             long aggPeriod,
                                             long domainAggTimestamp){
		this.avgTravelTime = avgTravelTime;
		this.sdTravelTime = sdTravelTime;
		this.numVehicles = numVehicles;
		this.aggPeriod = aggPeriod;
		this.linkid = linkid;
		this.domainAggTimestamp = domainAggTimestamp;
		this.aggTimestamp = System.currentTimeMillis();
		this.areaName = areaName;
	}

	public long getAggTimestamp() {
		return aggTimestamp;
	}

	public long getDomainAggTimestamp() {
		return domainAggTimestamp;
	}

	public void setDomainAggTimestamp(long domainAggTimestamp) {
		this.domainAggTimestamp = domainAggTimestamp;
	}

	public void setAggTimestamp(long aggTimestamp) {
		this.aggTimestamp = aggTimestamp;
	}

	public long getAggPeriod() {
		return aggPeriod;
	}

	public void setAggPeriod(long aggPeriod) {
		this.aggPeriod = aggPeriod;
	}

	public String getLinkid() {
		return linkid;
	}

	public void setLinkid(String linkid) {
		this.linkid = linkid;
	}

	public String getAreaName() {
		return areaName;
	}

	public void setAreaName(String areaName) {
		this.areaName = areaName;
	}

	public double getAvgTravelTime() {
		return avgTravelTime;
	}

	public void setAvgTravelTime(double avgTravelTime) {
		this.avgTravelTime = avgTravelTime;
	}

	public double getSdTravelTime() {
		return sdTravelTime;
	}

	public void setSdTravelTime(double sdTravelTime) {
		this.sdTravelTime = sdTravelTime;
	}

	public int getNumVehicles() {
		return numVehicles;
	}

	public void setNumVehicles(int numVehicles) {
		this.numVehicles = numVehicles;
	}

	public String toString(){
		return new Gson().toJson(this);
	}
}
