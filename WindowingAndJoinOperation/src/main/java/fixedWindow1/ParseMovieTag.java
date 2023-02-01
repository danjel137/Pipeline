package fixedWindow1;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.io.StringReader;

public class ParseMovieTag extends DoFn<String, MovieTag> {
    @ProcessElement
    public void apply(ProcessContext c)throws IOException{
        final CSVParser parser=new CSVParser(new StringReader(c.element()), CSVFormat.DEFAULT
                .withDelimiter(',')
                .withHeader(MovieTag.header_maping));

        CSVRecord record=parser.getRecords().get(0);

        if(record.get("userId").contains("userId")){
            return;
        }
        DateTimeZone timeZone= DateTimeZone.forID("UTC");
        DateTime startedWatching= LocalDateTime.parse(record.get("timestamp").trim(),
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toDateTime(timeZone);

        MovieTag movieTag=new MovieTag();
        movieTag.setUserId(record.get("userId"));
        movieTag.setSessionId(Integer.valueOf(record.get("sessionId")));
        movieTag.setMovieId(record.get("movieId").trim());
        movieTag.setTag(record.get("tag").trim());
        movieTag.setTimestamp(startedWatching.toInstant());

        c.output(movieTag);
    }
}
