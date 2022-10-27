package CustomPipelineOptions;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class AveragePriceProcessing {
    private static final String CSV_HEADER = "Date,Product,Price,Card,Country";

    public interface AveragePriceProcessingOptions extends PipelineOptions {
        @Description("Path of file to read from")
        @Default.String("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\AveragePriceProcesingInput.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of file to write to")
        @Default.String("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\AveragePriceProcesingOutput.csv")

            //@Validation.Required
        String getOutputFile();

        void setOutputFile(String value);

    }

    public static void main(String[] args) {


        AveragePriceProcessingOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .as(AveragePriceProcessingOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadCsv", TextIO.read().from(options.getInputFile()))
                .apply("Remove Header",ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("getProductAndPriceAsKV",ParDo.of(new ComputeAveragePriceFn()))
                .apply("AverageAggregate", Mean.perKey())
                .apply("FormatResult", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String,Double>productCount)->
                                productCount.getKey()+","+productCount.getValue()))
                .apply("Write result",TextIO.write().to(options.getOutputFile()).withNumShards(1));

        pipeline.run();
    }

}

