package InnerJoin;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class InnerJoinExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<KV<String, String>> pOrderCollection = pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\Join\\src\\main\\resources\\UserOrderInput.csv"))
                .apply(ParDo.of(new OrderParsing()));

        PCollection<KV<String, String>> pUserCollection = pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\Join\\src\\main\\resources\\UserInput.csv"))
                .apply(ParDo.of(new UserParsing()));

        final TupleTag<String> orderTuple = new TupleTag<String>();
        final TupleTag<String> userTuple = new TupleTag<String>();

        PCollection<KV<String, CoGbkResult>> result = KeyedPCollectionTuple.of(orderTuple, pOrderCollection)
                .and(userTuple, pUserCollection)
                .apply(CoGroupByKey.<String>create());

        PCollection<String> output = result.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
                String strKey = c.element().getKey();
                CoGbkResult valObject = c.element().getValue();

                Iterable<String> orderTable = valObject.getAll(orderTuple);
                Iterable<String> userTable = valObject.getAll(userTuple);

                for (String order : orderTable) {
                    for (String user : userTable) {
                        c.output(strKey + "," + order + "," + user);
                    }
                }
            }
        }));

        output.apply(TextIO.write().to("C:\\Users\\HP\\IdeaProjects\\Pipelines\\Join\\src\\main\\resources\\GroupByKey.csv").withNumShards(1));
        pipeline.run();
    }

}
