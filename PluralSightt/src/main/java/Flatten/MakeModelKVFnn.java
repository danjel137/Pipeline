package Flatten;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class MakeModelKVFnn extends DoFn<String, KV<String,String>> {
    @ProcessElement
    public void apply(ProcessContext c){
        String arr[]=c.element().split(",");
        String model= arr[8];
        String make=arr[0];

        c.output(KV.of(make,model));


    }
}
