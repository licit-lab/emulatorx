package model;

import com.google.gson.Gson;

public class PacketGenerator {
    public static String aggregateVehiclesTravelTimeSample(String linkid, String areaName, double avgTravelTime, double sdTravelTime,
                                                           int numVehicles,
                                                           long aggPeriod,
                                                           long domainAggTimestamp) {
        AggregateVehiclesTravelTimeSample sample = new AggregateVehiclesTravelTimeSample(linkid,areaName,avgTravelTime,sdTravelTime,
                numVehicles,aggPeriod,domainAggTimestamp);
        return new Gson().toJson(sample);
    }
}

