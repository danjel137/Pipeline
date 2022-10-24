package parDo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class ParDoExample {
    public static void main(String[] args) {
        Pipeline pipeline=Pipeline.create();
        PCollection<String>pCustomersInput=pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\parDoInput.csv"));
        PCollection<String>pCustomersMap=pCustomersInput.apply(ParDo.of(new Userr()));
        pCustomersMap.apply(TextIO.write().to("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\parDoInput.csv").withHeader("Id,Name,Last,Name,City").withNumShards(1));
        pipeline.run();
    }
}
