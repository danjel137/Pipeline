package mapElementsExample;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapElementsExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pListFromCsv = pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\name.csv"));
        PCollection<String> pTransform = pListFromCsv.apply(MapElements.into(TypeDescriptors.strings()).via((String s) -> s.toUpperCase()));
         pTransform.apply(TextIO.write().to("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\names.csv").withNumShards(1));
        pipeline.run();
    }
}
