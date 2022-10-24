import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Countt {
    public static void main(String[] args) {
        Pipeline pipeline=Pipeline.create();
        PCollection <String>inputFromCv=pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\Agregation\\src\\main\\resources\\distinctInput.csv"));

        PCollection<String> distinctValues=inputFromCv.apply(Distinct.<String>create());
        PCollection <Long>countPersons=distinctValues.apply(Count.globally());

        countPersons.apply(ParDo.of(new DoFn<Long,Void>(){
            @ProcessElement
            public void apply(ProcessContext c){
                System.out.println(c.element());
            }
        }));

        pipeline.run();

    }
}
