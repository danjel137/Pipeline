package innerJoin;

import demo6.RemoveHeader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

public class InnerJoin {
    public static String headerCustomersInfo = "CustomerID,Gender,Age,Annual_Income";
    public static String headerCustomersScore = "CustomerID,Spending Score";

    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<KV<String, String>> customerGender = pipeline.apply("ReadFromCustomersInfo", TextIO.read().
                        from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\WindowingAndJoinOperation\\src\\main\\resources\\customersInfoInput"))
                .apply("FilterHeader", ParDo.of(new RemoveHeaderrr(headerCustomersScore)))
                .apply("IdGenderKV", ParDo.of(new IdGenderKV()));

        PCollection<KV<String, Integer>> customersScore = pipeline.apply("ReadfromCustomersScore", TextIO.read()
                        .from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\WindowingAndJoinOperation\\src\\main\\resources\\customersScore"))
                .apply("FilterHeader", ParDo.of(new RemoveHeaderrr(headerCustomersInfo)))
                .apply("IdScoreKv", ParDo.of(new IdScoreKV()));

//        PCollection<KV<String, KV<String, Integer>>> innerJoin = Join.innerJoin(customerGender, customersScore);
//
//
//        PCollection<KV<String, KV<String, Integer>>> joinedDatasets = org.apache.beam.sdk.extensions.joinlibrary.Join.innerJoin(
//                customerGender, customersScore);
//
//        innerJoin.apply(MapElements.via(new SimpleFunction<KV<String, KV<String, Integer>>, Void>() {
//            @Override
//            public Void apply(KV<String, KV<String, Integer>> input) {
//
//                System.out.println(input.getKey() + ", " + input.getValue().getKey() + ", " + input.getValue().getValue());
//                return null;
//            }
//        }));



//        final PCollectionView<Map<String, Integer>> customersView = customersScore.apply(View.asMap());
//
//        customerGender.apply(ParDo.of(new DoFn<KV<String, String>, String>() {
//            @ProcessElement
//            public void apply(ProcessContext c) {
//                Map<String, Integer> customersScore = c.sideInput(customersView);
//                KV<String, String> element = c.element();
//
//                Integer score = customersScore.get(element.getKey());
//                System.out.println(element.getKey() + ", " + element.getValue() + ", " + score);
//            }
//        }).withSideInput(customersView));
//        pipeline.run();
   }
}
