import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class Appender {
	public static void main(String[] args){
		String areas = "all_links.csv";
		String query = "        \"neo4j.topic.cypher.Chatillon-Northbound\": \"MERGE ()-[s:STREET {linkId: event.linkId}]->() SET s.avgTravelTime = event.sample.avgTravelTime,s.sdTravelTime = event.sample.sdTravelTime,s.numVehicles = event.sample.numVehicles,s.timestamp = event.timestamp,s.aggPeriod = duration({seconds: event.sample.aggPeriod.seconds,nanoseconds: event.sample.aggPeriod.nanos}),s.startTime = localdatetime({year: event.sample.startTime.date.year,month: event.sample.startTime.date.month,day: event.sample.startTime.date.day,hour: event.sample.startTime.time.hour,minute: event.sample.startTime.time.minute,second: event.sample.startTime.time.second,nanosecond: event.sample.startTime.time.nano}),s.endTime = localdatetime({year: event.sample.endTime.date.year,month: event.sample.endTime.date.month,day: event.sample.endTime.date.day,hour: event.sample.endTime.time.hour,minute: event.sample.endTime.time.minute,second: event.sample.endTime.time.second,nanosecond: event.sample.endTime.time.nano})\",";
		String topics = "       \"topics\": \"";
		try {
			/*Writer writerNeo4jSink = Files.newBufferedWriter(Paths.get("all_links_edited.csv"));
			BufferedReader reader = Files.newBufferedReader(Paths.get(areas));
			String line;
			while ((line = reader.readLine()) != null) {
				if(line.contains("''"))
					line = line.replace("''","'");
				writerNeo4jSink.write(line+"\n");
			}
			writerNeo4jSink.close();
			reader.close();*/
			Writer writerAreas = Files.newBufferedWriter(Paths.get("neo4j-sink.json"), StandardOpenOption.APPEND);
			Reader reader = Files.newBufferedReader(Paths.get("all_links.csv"));
			Set<String> areaslist = new HashSet<>();
			CSVFormat csvFormat = CSVFormat.DEFAULT.withFirstRecordAsHeader().withDelimiter(';');
			CSVParser csvParser = csvFormat.parse(reader);
			for (CSVRecord r: csvParser){
				String s = r.get("areaname");
				String input = StringUtils.stripAccents(s).replace("'","_").replace(" ","_");
				System.out.println(input);
				areaslist.add(input);
			}
			/*System.out.println(areaslist.size());
			for(String s: areaslist){
				writerAreas.write(s+"\n");
			}*/
			StringBuilder builder = new StringBuilder();
			builder.append(topics);
			for(String s: areaslist){
				builder.append(s);
				builder.append("-Northbound");
				builder.append(",");
			}
			String topics_final = builder.toString();
			writerAreas.write("\n");
			writerAreas.write(topics_final+",");
			for(String s: areaslist){
				writerAreas.write(query.replace("Chatillon",s)+"\n");
			}
			writerAreas.write("  }\n");
			writerAreas.write("}");


			writerAreas.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String validateTopicName(String topicName) {
		return topicName.replace("é", "e")
				.replace(" ", "_")
				.replace("'", "_")
				.replace("é", "e")
				.replace("è", "e")
				.replace("è","e")
				.replace("à", "a")
				.replace("ô", "o")
				.replace("É", "E")
				.replace("É","E")
				.replace("À", "A")
				.replace("â","a")
				.replace("ô","o");
	}
}
