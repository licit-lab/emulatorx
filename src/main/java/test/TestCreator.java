package test;

import link.Link;
import link.STLink;
import node.STAggregateTravelTimeAreaNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

public class TestCreator {
	public static final String id = "344750";
	public static final String coverage = "0,35";
	public static final String timestamp = "06/09/2018 00:01:00";
	public static final String speed = "100";

	public static void main(String[] args){
		String links = "all_links.csv";
		String changedLinks = "prova.csv";
		String testSamples = args[0];
		try {
			//TestCreator.setTopic(links,changedLinks);
			BufferedWriter testSamplesWriter = Files.newBufferedWriter(Paths.get(testSamples));
			BufferedReader reader = Files.newBufferedReader(Paths.get(changedLinks));
			CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter(';').withFirstRecordAsHeader();
			CSVParser csvParser = csvFormat.parse(reader);
			CSVPrinter csvPrinter = CSVFormat.DEFAULT.withDelimiter(';').withHeader("id","linkid","coverage","timestamp","speed").print(testSamplesWriter);
			Iterator<CSVRecord> iterator = csvParser.iterator();
			for(int i = 0; i < Integer.parseInt(args[1]); i++){
				CSVRecord r = iterator.next();
				csvPrinter.printRecord(TestCreator.id,r.get("id"),TestCreator.coverage,TestCreator.timestamp,TestCreator.speed);
			}
			csvPrinter.flush();
			testSamplesWriter.close();
		}catch (Exception e){

		}
	}

	private static void setTopic(String links, String changedLinks){
		try{
			BufferedWriter editedLinks = Files.newBufferedWriter(Paths.get(changedLinks));
			BufferedReader reader = Files.newBufferedReader(Paths.get(links));
			CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter(';').withFirstRecordAsHeader();
			CSVParser csvParser = csvFormat.parse(reader);
			CSVPrinter csvPrinter = CSVFormat.DEFAULT.withDelimiter(';').withHeader("id","length","ffs","speedlimit","frc",
					"netclass","fow","routenumber","areaname","name","geom").print(editedLinks);
			for (CSVRecord r: csvParser){
				csvPrinter.printRecord(r.get("id"),r.get("length"),r.get("ffs"),r.get("speedlimit"),r.get("frc"),
						r.get("netclass"),r.get("fow"),
						r.get("routenumber"),"Chatillon",r.get("name"),r.get("geom"));
			}
			csvPrinter.flush();
			editedLinks.close();
		}catch (Exception e){

		}

	}
}
