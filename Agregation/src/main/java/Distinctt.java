import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Distinctt {
    public static void main(String[] args) {
        Pipeline pipeline=Pipeline.create();
        PCollection <String>inputValues=pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\Agregation\\src\\main\\resources\\distinctInput.csv"));

        PCollection<String> distinctValues=inputValues.apply(Distinct.<String>create());
        distinctValues.apply(ParDo.of(new DoFn<String,String>() {
            @ProcessElement
            public void apply(ProcessContext c){
                System.out.println(c.element());
            }
        }));

        distinctValues.apply(TextIO.write().to("C:\\Users\\HP\\IdeaProjects\\Pipelines\\Agregation\\src\\main\\resources\\distinctOutput.csv").withNumShards(1));
        pipeline.run();
    }
}
