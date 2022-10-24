package groupBy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class Groupbyy {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCustomerOrderList = pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\Agregation\\src\\main\\resources\\groupByInput.cv"));

        PCollection<KV<String, Integer>> personsWithKV = pCustomerOrderList.apply(ParDo.of(new SelectPersonToKV()));

        PCollection<KV<String, Iterable<Integer>>> KVOrder = personsWithKV.apply(GroupByKey.<String, Integer>create());

        PCollection<String> output = KVOrder.apply(ParDo.of(new SelectedPersonsWithHisTotalSum()));

        output.apply(TextIO.write().to("C:\\Users\\HP\\IdeaProjects\\Pipelines\\Agregation\\src\\main\\resources\\groupByOutput.cv").withNumShards(1));

        pipeline.run();
    }
}
