package timestamp;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class kot {
    public static void main(String[] args) {
        final argsInput options = PipelineOptionsFactory.fromArgs(args).withValidation().as(argsInput.class);
        Pipeline p = Pipeline.create(options);
        PCollection<String>read=p.apply(TextIO.read().from(options.getInputFilePattern()))
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void aVoid(ProcessContext c){
                        System.out.println(c.element());
                    }
                }));
p.run();
    }
}
