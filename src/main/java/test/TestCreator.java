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
		//String links = "all_links.csv";
		String changedLinks = "prova.csv";
		String samples = args[0];
		System.out.println(samples);
		String links = args[1];
		System.out.println(links);
		System.out.println(args[2]);
		try {
			//TestCreator.setTopic(links,changedLinks);
			BufferedReader reader = Files.newBufferedReader(Paths.get(changedLinks));
			BufferedWriter samplesWriter = Files.newBufferedWriter(Paths.get(samples));
			BufferedWriter linksWriter = Files.newBufferedWriter(Paths.get(links));
			CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter(';').withFirstRecordAsHeader();
			CSVParser csvParser = csvFormat.parse(reader);
			CSVPrinter csvPrinterSample = CSVFormat.DEFAULT.withDelimiter(';')
					.withHeader("id","linkid","coverage","timestamp","speed").print(samplesWriter);
			CSVPrinter csvPrinterLink = CSVFormat.DEFAULT.withDelimiter(';')
					.withHeader("id","length","ffs","speedlimit","frc","netclass","fow","routenumber","areaname","name","geom").print(linksWriter);
			Iterator<CSVRecord> iterator = csvParser.iterator();
			for(int i = 0; i < Integer.parseInt(args[2]); i++){
				CSVRecord r = iterator.next();
				csvPrinterSample.printRecord(TestCreator.id,r.get("id"),TestCreator.coverage,TestCreator.timestamp,TestCreator.speed);
				csvPrinterLink.printRecord(r.get("id"),r.get("length"),r.get("ffs"),r.get("speedlimit"),r.get("frc"),
						r.get("netclass"),r.get("fow"),r.get("routenumber"),r.get("areaname"),r.get("name"),r.get("geom"));
			}
			csvPrinterSample.flush();
			samplesWriter.close();
			csvPrinterLink.flush();
			linksWriter.close();
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
