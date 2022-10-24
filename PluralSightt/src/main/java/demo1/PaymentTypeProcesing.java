package demo1;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;
import java.util.List;

public class PaymentTypeProcesing {
    final static List<String>LINES= Arrays.asList(
            "1/5/09 5:30,Shoes,1200,Amex,Netherlands",
            "1/2/09 9:16,Jacket,1200,MasterCard,United States",
            "1/5/09 10:08,Phone,3600,Visa,UnitedStates");

    public static void main(String[] args) {

        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);
        pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
                .apply(ParDo.of(new ExtractPaymentTypeFn()))
                .apply(ParDo.of(new PrintToConsoleFn()));
        pipeline.run();
    }



}
