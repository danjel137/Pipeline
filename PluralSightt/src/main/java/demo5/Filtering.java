package demo5;

import CustomPipelineOptions.FilterHeaderFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

public class Filtering {
    public static final String CSV_HEADER="car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);

        pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\car_Input.csv"))
                .apply("filtering header", ParDo.of(new FilterHeaderFn(null)))
                .apply("FilterSedanHatchback",ParDo.of(new FilterSedanHatchback()))
                .apply("Filterprice",ParDo.of(new FilterPrice(2000.0)))
                .apply(TextIO.write().to("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\car_Output.csv").withNumShards(1));

        pipeline.run();

    }
}
