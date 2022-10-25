package compositeTransform;

import GroupByKey.MakePriceKVFn;
import demo3.FilterHeaderFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


public class MakeKVTransform extends PTransform <
        PCollection<String>,PCollection<KV<String,Double>>>{

    @Override
    public PCollection<KV<String, Double>> expand(PCollection<String> input) {
        return input.apply("FilterHeader", ParDo.of(new FilterHeaderFn("car,price,body,mileage,engV,engType,registration,year,model,drive")))
                .apply("MakePriceKVFn",ParDo.of(new MakePriceKVFn()));
    }
}
