package model;

import com.google.gson.Gson;

public class AggregateVehiclesTravelTimeSample{
	private double avgTravelTime;
	private double sdTravelTime;
	private  int numVehicles;
	private long aggPeriod, aggTimestamp;
	private String startDateTime, linkid, areaName;

	public AggregateVehiclesTravelTimeSample(String linkid, String areaName, double avgTravelTime, double sdTravelTime,
                                             int numVehicles,
                                             long aggPeriod,
                                             String startDateTime,
                                             long aggTimestamp){
		this.avgTravelTime = avgTravelTime;
		this.sdTravelTime = sdTravelTime;
		this.numVehicles = numVehicles;
		this.aggPeriod = aggPeriod;
		this.linkid = linkid;
		this.startDateTime = startDateTime;
		this.aggTimestamp = aggTimestamp;
		this.areaName = areaName;
	}

	public long getAggPeriod() {
		return aggPeriod;
	}

	public void setAggPeriod(long aggPeriod) {
		this.aggPeriod = aggPeriod;
	}

	public long getAggTimestamp() {
		return aggTimestamp;
	}

	public void setAggTimestamp(long aggTimestamp) {
		this.aggTimestamp = aggTimestamp;
	}

	public String getStartDateTime() {
		return startDateTime;
	}

	public void setStartDateTime(String startDateTime) {
		this.startDateTime = startDateTime;
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
