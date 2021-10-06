package test;

import model.PacketGenerator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

public class SyntheticDatasetGenerator1 {
    public static void main(String[] args){
        Set<String> areas = new HashSet<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        Reader reader;
        int numObs = 1161;
        try {
            Path path = Paths.get(".\\prova2");
            Files.createDirectories(path);
            reader = Files.newBufferedReader(Paths.get("nlinks_area.csv"));
            CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';');
            CSVParser csvParser = csvFormat.parse(reader);
            for (CSVRecord r: csvParser)
                areas.add(r.get("NOM_COM"));
            reader.close();
            for(String area: areas){
                PrintWriter pw = new PrintWriter(new FileWriter("prova2\\"+area+".txt", true));
                pw.println(numObs);
                for(int i = 0; i < numObs; i++){
                    double avgTravelTime = 100.3; //add mean
                    String endDateTimeString = "06/09/2018 00:05:00";
                    LocalDateTime endDateTime = LocalDateTime.parse(endDateTimeString,formatter);
                    LocalDateTime aggregationDateTime = endDateTime.minusSeconds(1); //add time
                    Instant instant = aggregationDateTime.atZone(ZoneId.systemDefault()).toInstant(); //add aggregation
                    long domainAggTimestamp = instant.toEpochMilli();
                    String aggregateVehiclesTravelTime =
                            PacketGenerator.aggregateVehiclesTravelTimeSample("12500009826508", area,
                                avgTravelTime, avgTravelTime, 1,
                                (3*60-1)*1000,domainAggTimestamp);
                    pw.println(aggregateVehiclesTravelTime);

                }
                pw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
