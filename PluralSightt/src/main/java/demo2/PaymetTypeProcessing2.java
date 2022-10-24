package demo2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PaymetTypeProcessing2 {
    final static List<String> LINES= Arrays.asList(
            "1/5/09 5:30,Shoes,1200,Amex,Netherlands",
            "1/2/09 9:16,Jacket,1200,MasterCard,United States",
            "1/5/09 10:08,Phone,3600,Visa,UnitedStates");
    public static void main(String[] args) {

        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);

        pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
                .apply("PrintOutput", MapElements.via(new SimpleFunction<String,String>(){
                    @Override
                    public String apply(String input){
                        System.out.println(input);
                        return input;
                    }
                }))
                .apply("ExtactPaymentType", FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line)-> Collections.singletonList(line.split(",")[3])))
                .apply("CountPerElement", Count.perElement())
                .apply("FormatResult",MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String,Long> typeCount)->typeCount.getKey()+","+typeCount.getValue()))
                .apply("PrintExtractOutput",MapElements.via(new SimpleFunction<String, Void>() {
                    @Override
                    public Void apply(String input){
                        System.out.println(input);
                        return null;
                    }
                }))


        ;


        pipeline.run();
    }

}
