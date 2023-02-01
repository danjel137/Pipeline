package creatingAndExecutingPipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

public class PaymentTypeProcesing {
    final static List<String>LINES= Arrays.asList(
            "1/5/09 5:30,Shoes,1200,Amex,Netherlands",
            "1/2/09 9:16,Jacket,1200,MasterCard,United States",
            "1/5/09 10:08,Phone,3600,Visa,UnitedStates");



    public static void main(String[] args) {
        Instant now = new Instant(0);
        org.joda.time.Instant sec1Duration = now.plus(org.joda.time.Duration.standardSeconds(1));
        Instant sec2Duration = now.plus(Duration.standardSeconds(2));

        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);
        PCollection<String> timestampedLetters =
                pipeline.apply(Create.timestamped(Arrays.asList(
                        TimestampedValue.of("a", now),
                        TimestampedValue.of("a", sec1Duration),
                        TimestampedValue.of("a", sec1Duration),
                        TimestampedValue.of("a", sec1Duration),
                        TimestampedValue.of("b", sec2Duration),
                        TimestampedValue.of("a", sec1Duration),
                        TimestampedValue.of("a", sec1Duration),
                        TimestampedValue.of("a", sec1Duration)
                )));
        Duration windowDuration = Duration.standardSeconds(1);

        Window<String> window = Window.into(FixedWindows.of(windowDuration));
        PCollection<String> countResult = timestampedLetters.apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                        processContext.outputWithTimestamp(processContext.element(), processContext.timestamp().plus(1));
                    }
                }))
                .apply(window)
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via((String letter) -> KV.of(letter, 1)))
                .apply(Count.perKey())
                .apply(MapElements.into(TypeDescriptors.strings()).via((KV<String, Long> pair) ->
                        pair.getKey() + "=" + pair.getValue()));


//        pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
//                .apply(ParDo.of(new ExtractPaymentTypeFn()))
//                .apply(ParDo.of(new PrintToConsoleFn()));

        countResult.apply(ParDo.of(new DoFn<String, Void>() {

            @ProcessElement
            public void kot(ProcessContext c){

               // System.out.println(c.element());
            }
        }));

        PCollection<KV<String, Integer>> input = pipeline.apply(Create.of(
                KV.of("first", 1),
                KV.of("second", 2),
                KV.of("third", 3)
        ));



        pipeline.run();
    }



}
