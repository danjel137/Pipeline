package flatternExample;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlatternExample {
    public static void main(String[] args) {
        Pipeline pipeline =Pipeline.create();
        PCollection <String> flattern1=pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\flattern1Input.csv"));
        PCollection <String> flattern2=pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\flattern2Input.csv"));
        PCollection <String> flattern3=pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\flattern3Input.csv"));

        PCollectionList <String> listFlatten= PCollectionList.of(flattern1).and(flattern2).and(flattern3);
        PCollection<String>merge=listFlatten.apply(Flatten.pCollections());
        merge.apply(TextIO.write().to("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\mergeflatternInput.csv").withNumShards(1));
        pipeline.run();
    }
}
