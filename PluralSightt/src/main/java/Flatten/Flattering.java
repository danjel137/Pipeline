package Flatten;

import demo6.RemoveHeader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Flattering {
    private static final String CSV_HEADER="car,price,body,mileage,engV,engType,registration,year,model,drive";

    public static void main(String[] args) {
        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);

        PCollection<String>car1=pipeline.apply(TextIO.read()
                .from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\car1Input.csv"));

        PCollection<String>car2=pipeline.apply(TextIO.read()
                .from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\car2Input.csv"));

        PCollection<String>car3=pipeline.apply(TextIO.read()
                .from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\PluralSightt\\src\\main\\resources\\car3Input.csv"));

        PCollectionList<String> pCarList=PCollectionList.of(car1).and(car2).and(car3);

        PCollection<String>flattenedCollection=pCarList.apply(Flatten.pCollections());

        flattenedCollection.apply("Skip Header", ParDo.of(new RemoveHeaderr(CSV_HEADER)))
                .apply("MakeModelKV",ParDo.of(new MakeModelKVFnn()))
                .apply("CountPerKEy", Count.perKey())
                .apply("PrintToConsole",ParDo.of(new DoFn<KV<String, Long>, Void>() {
                    @ProcessElement
                    public void apply(ProcessContext c){
                        System.out.println(c.element().getKey()+": "+c.element().getValue());
                    }
                }));

        pipeline.run();


    }
}
