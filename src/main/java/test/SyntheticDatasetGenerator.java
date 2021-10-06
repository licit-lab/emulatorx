package test;

import model.PacketGenerator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import util.SettingReader;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.DoubleBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class SyntheticDatasetGenerator {
    public static void main(String[] args){
        HashMap<String,Integer> counters = new HashMap<>();
        HashMap<String,Integer> actualCounters = new HashMap<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        double FACTOR_M2KM = 0.001;
        double FACTORH_2SEC = 3600;
        SettingReader st = new SettingReader();
        String linkFilePath = st.readElementFromFileXml("settings.xml", "Files", "links");
        Reader reader;
        double perc = 0.5;
        try {
            Path path = Paths.get(".\\halved");
            Files.createDirectories(path);
            reader = Files.newBufferedReader(Paths.get("nlinks_area.csv"));
            CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';');
            CSVParser csvParser = csvFormat.parse(reader);
            for (CSVRecord r: csvParser){
                PrintWriter pw = new PrintWriter(new FileWriter("halved\\"+r.get("NOM_COM")+".txt", true));
                int count = Integer.parseInt(r.get("nlinks"));
                double percCountDouble = (count * perc);
                int percCount = (int) Math.round(percCountDouble);
                counters.put(r.get("NOM_COM"),percCount);
                actualCounters.put(r.get("NOM_COM"),0);
                pw.println(percCount);
                pw.close();
            }
            reader.close();
            reader = Files.newBufferedReader(Paths.get(linkFilePath));
            csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';');
            csvParser = csvFormat.parse(reader);
            for(CSVRecord r: csvParser){
                String areaName = r.get("NOM_COM");
                System.out.println(areaName);
                if(actualCounters.get(areaName) < counters.get(areaName)){
                    double avgTravelTime = ((Double.parseDouble(r.get("length"))*FACTOR_M2KM)/Double.parseDouble(r.get("ffs")))*FACTORH_2SEC; //add mean
                    double sdTravelTime = avgTravelTime;
                    String endDateTimeString = "06/09/2018 00:05:00";
                    LocalDateTime endDateTime = LocalDateTime.parse(endDateTimeString,formatter);
                    LocalDateTime aggregationDateTime = endDateTime.minusSeconds(1); //add time
                    Instant instant = aggregationDateTime.atZone(ZoneId.systemDefault()).toInstant(); //add aggregation
                    long domainAggTimestamp = instant.toEpochMilli();
                    String aggregateVehiclesTravelTime = PacketGenerator.aggregateVehiclesTravelTimeSample(r.get("id"), r.get("NOM_COM"),
                            avgTravelTime, sdTravelTime, 1,
                            (3*60-1)*1000,domainAggTimestamp);

                    PrintWriter pw = new PrintWriter(new FileWriter("halved\\"+r.get("NOM_COM")+".txt", true));
                    pw.println(aggregateVehiclesTravelTime);
                    pw.close();
                    actualCounters.put(areaName,actualCounters.get(areaName)+1);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
