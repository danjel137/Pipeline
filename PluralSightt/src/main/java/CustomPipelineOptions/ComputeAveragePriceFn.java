package CustomPipelineOptions;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ComputeAveragePriceFn extends DoFn<String, KV<String,Double>> {
    @ProcessElement
    public void apply(ProcessContext c){
        String[]data=c.element().split(",");
        String product=data[1];
        Double price =Double.parseDouble(data[2]);
        c.output(KV.of(product,price));
    }
}
