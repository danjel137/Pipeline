package demo6;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class Computations {

    public static void main(String[] args) {
        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);
        pipeline.apply("readPath",TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\car_Input.csv"))
                .apply("removeHeder", ParDo.of(new RemoveHeader()))
                .apply("getDateCreatedAndAge",ParDo.of(new MakeAgeFn()))
                .apply("PritToCosole",ParDo.of(new DoFn<KV<String, Integer>, Void>() {
                    @ProcessElement
                    public void apply(ProcessContext c){
                        System.out.println(c.element().getKey()+": "+c.element().getValue());
                    }
                }));
        pipeline.run();
    }


}
