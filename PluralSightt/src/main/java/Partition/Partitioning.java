package Partition;

import GroupByKey.MakePriceKVFn;
import demo3.FilterHeaderFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Partitioning {
    public static String header="car,price,body,mileage,engV,engType,registration,year,model,drive";
    public static void main(String[] args) {
        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);


        PCollection<KV<String,Double>>makePRiceKv=pipeline
                .apply("ReadAds", TextIO.read()
                        .from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\car_Input.csv"))
                .apply("FilterHeader", ParDo.of(new FilterHeaderFn(header)))
                .apply("MakePriceKv",ParDo.of(new MakePriceKVFn()));

        PCollectionList<KV<String,Double>>priceCategories=makePRiceKv
                .apply(Partition.of(
                        4, new Partition.PartitionFn<KV<String, Double>>() {
                            @Override
                            public int partitionFor(KV<String, Double> elem, int numPartitions) {
                                if(elem.getValue()<2000){
                                    return 0;
                                }else if (elem.getValue()<5000){
                                    return 1;
                                }else if (elem.getValue()<10000){
                                    return 2;
                                }
                                    return 3;
                            }
                        }
                ));

        priceCategories.get(1)
                .apply("PrintToConsole",ParDo.of(new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void apply(ProcessContext c){
                        System.out.println(c.element()+": "+c.element().getValue());
                    }
                }));
        pipeline.run().waitUntilFinish();
    }
}
