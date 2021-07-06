package model;

import com.google.gson.Gson;

public class PacketGenerator {
    public static String aggregateVehiclesTravelTimeSample(String linkid, String areaName, double avgTravelTime, double sdTravelTime,
                                                           int numVehicles,
                                                           long aggPeriod,
                                                           String startDateTime,
                                                           long aggTimestamp) {
        AggregateVehiclesTravelTimeSample sample = new AggregateVehiclesTravelTimeSample(linkid,areaName,avgTravelTime,sdTravelTime,
                numVehicles,aggPeriod,startDateTime,aggTimestamp);
        return new Gson().toJson(sample);
    }
}

