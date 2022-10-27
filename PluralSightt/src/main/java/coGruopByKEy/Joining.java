package coGruopByKEy;

import CustomPipelineOptions.FilterHeaderFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class Joining {
    private  static final String csv_info_header="CustomerId,Gender,Age,Annual_Income";
    private  static final String csv_score_header="CustomersID,Spending Score";

    public static void main(String[] args) {
        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);

        PCollection <KV<String,Integer>> customersIncome=pipeline
                .apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\customersInfo"))
                .apply("RemoveHedaer", ParDo.of(new FilterHeaderFn(csv_info_header)))
                .apply("IDIncome", ParDo.of(new IdIncomeKVFn()));

        customersIncome.apply("PrintToConsole",ParDo.of(new DoFn<KV<String, Integer>, Void>() {
            @ProcessElement
            public void apply(ProcessContext c){
                System.out.println("Income "+c.element().getKey()+": "+c.element().getValue());
            }
        }));

        PCollection <KV<String,Integer>> customerScore=pipeline
                .apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\costumers_score"))
                .apply("FilterScore",ParDo.of(new FilterHeaderFn(csv_score_header)))
                .apply("IdScreKV",ParDo.of(new IdScoreKVFn()));

        final TupleTag<Integer>incomeTag=new TupleTag<>();
        final TupleTag<Integer>scoreTag=new TupleTag<>();

        PCollection<KV<String, CoGbkResult>>joined= KeyedPCollectionTuple
                .of(incomeTag,customersIncome)
                .and(scoreTag,customerScore)
                .apply(CoGroupByKey.create());

        joined.apply(ParDo.of(
                new DoFn<KV<String, CoGbkResult>, String>() {
                    @ProcessElement
                    public void apply(
                            @Element KV<String,CoGbkResult>element,
                            OutputReceiver<String>out){
                                String id=element.getKey();
                                Integer income=element.getValue().getOnly(incomeTag);
                                Integer spendimgScore=element.getValue().getOnly(scoreTag);

                                out.output(id+","+income+","+spendimgScore);
                                //System.out.println(id+","+income+","+spendimgScore);
                    }

                }
        ));
        pipeline.run();

    }
}
