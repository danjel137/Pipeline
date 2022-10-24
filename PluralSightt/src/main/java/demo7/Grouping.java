package demo7;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.util.logging.Logger;

public class Grouping{
    private static final Logger logger= Logger.getLogger(Grouping.class.getName());
    private static final String header="car,price,body,mileage,engV,engType,registration,year,model,drive";
    public static void main(String[] args) {
        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);

        pipeline.apply("ReadAds", TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\car_Input.csv"))
                .apply("FilterHeader", ParDo.of(new RemoveHeader(header)))
                .apply("Create CarModel PCollection", ParDo.of(new DoFn<String, CarModel>() {
                    @ProcessElement
                    public void processElement(ProcessContext c){
                        String []field=c.element().split(",");
                        c.output(new CarModel(field[0]+"_"+field[8], Double.parseDouble(field[1])));
                    }
                }))
                .apply("MakeModelKVFn", ParDo.of(new MakeModelKVFn()))
                .apply("MakeModelGrouping", GroupByKey.create())
                .apply("Average",ParDo.of(new ComputerAveragePriceFn()))
                .apply("PrintToConsole", ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        logger.info(c.element().getKey() + ": " + c.element().getValue());
                    }
                }));
        pipeline.run();

    }
}
