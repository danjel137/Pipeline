package demo7;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class MakePriceKVFn extends DoFn<String, KV<String,Double>> {
    @ProcessElement
    public void apply(ProcessContext c){
        String[]fields=c.element().split(",");

        String make=fields[0];
        Double price=Double.parseDouble(fields[1]);
        c.output(KV.of(make,price));
    }
}

