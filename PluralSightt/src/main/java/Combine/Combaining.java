package Combine;

import CustomPipelineOptions.FilterHeaderFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class Combaining {
    private  static final String csv_info_header="CustomerId,Gender,Age,Annual_Income";

    public static void main(String[] args) {
        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);

        PCollection<Double>ages=pipeline.apply(TextIO.read()
                .from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\customersInfo"))
                .apply("removeHeader", ParDo.of(new FilterHeaderFn(csv_info_header)))
                .apply("ExtractAge",ParDo.of(new ExtractAgeFn()));

        ages.apply("CombineAggregation", Combine.globally(new AverageFn()))
                .apply("PrintConsole",ParDo.of(new DoFn<Double, Void>() {
                    @ProcessElement
                    public void apply(ProcessContext c){
                        System.out.println(c.element());
                    }
                }));



        PCollection<KV<String, Double>> genderages=pipeline.apply(TextIO.read()
                        .from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\customersInfo"))
                .apply("removeHeader", ParDo.of(new FilterHeaderFn(csv_info_header)))
                .apply("ExtractAge",ParDo.of(new GenderAgeFn()));

        genderages.apply("CombinePerKEyAggregation",Combine.perKey(new AverageFn()))
                .apply("PrintToConsole",ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void apply(ProcessContext c){
                        System.out.println("Average is: "+c.element().getKey()+","+c.element().getValue());
                    }
                }));
        pipeline.run();

    }
}
