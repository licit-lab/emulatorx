import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import util.SettingReader;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Counter {
	public static void main(String[] args){
		int coverageZero = 0;
		int speedZero = 0;
		int coverageAndSpeedZero = 0;

		//Setting obs file path
		SettingReader st = new SettingReader();
		String obsFilePath = st.readElementFromFileXml("settings.xml", "Files", "observations");
		int count = 0;
		Reader reader;
		Writer writerCoverage, writerSpeed, writerCoverageSpeed;
		try {
			reader = Files.newBufferedReader(Paths.get(obsFilePath));
			writerCoverage = Files.newBufferedWriter(Paths.get("coverage.csv"));
			writerSpeed = Files.newBufferedWriter(Paths.get("speed.csv"));
			writerCoverageSpeed = Files.newBufferedWriter(Paths.get("coveragespeed.csv"));
			CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';');
			CSVParser csvParser = csvFormat.parse(reader);
			CSVPrinter coveragePrinter = CSVFormat.DEFAULT.print(writerCoverage);
			CSVPrinter speedPrinter = CSVFormat.DEFAULT.print(writerSpeed);
			CSVPrinter coverageSpeedPrinter = CSVFormat.DEFAULT.print(writerCoverageSpeed);

			for (CSVRecord r: csvParser) {
				count++;
				float coverage = Float.parseFloat(r.get(2).replace(',','.'));
				float sampleSpeed = Float.parseFloat(r.get(4));
				if(coverage == 0 && sampleSpeed == 0){
					coverageAndSpeedZero++;
					//coverageSpeedPrinter.printRecord(r.get(0),r.get(1),r.get(2),r.get(3),r.get(4));
				} else if(coverage == 0){
					coverageZero++;
					//coveragePrinter.printRecord(r.get(0),r.get(1),r.get(2),r.get(3),r.get(4));
				} else if(sampleSpeed == 0){
					speedZero++;
					//speedPrinter.printRecord(r.get(0),r.get(1),r.get(2),r.get(3),r.get(4));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("There are " + coverageZero + " samples with coverage set to zero.");
		System.out.println("There are " + speedZero + " samples with speed set to zero.");
		System.out.println("There are " + coverageAndSpeedZero + " samples with both coverage and speed set to zero.");
		System.out.println("There are in total " + count + " samples.");

	}
}
