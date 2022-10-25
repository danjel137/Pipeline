package compositeTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class Composite {
    public static void main(String[] args) {
        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);
        pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\car_Input.csv"))
                .apply("Composite",(new MakeKVTransform()))
                .apply("Average Price", Mean.perKey())
                .apply("Print Console",ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void apply(ProcessContext processContextc){
                        System.out.println(processContextc.element().getKey()+": "+processContextc.element().getValue());
                    }
                }));

        pipeline.run();


    }


}
