package mapElementsSimpleFunction;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;

public class MapElementsSimpleFunction {

    public static void main(String[] args) {
        Pipeline pipeline=Pipeline.create();
        PCollection<String>pCustomersInput=pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\mapElementsSimpleFunctionInput.csv"));
        PCollection<String>pCustomersMap=pCustomersInput.apply(MapElements.via(new User()));
        pCustomersMap.apply(TextIO.write().to("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\mapElementsSimpleFunctionOutput.csv").withNumShards(1));
        pipeline.run();
    }
}
