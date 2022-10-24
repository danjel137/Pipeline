package groupBy;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class SelectPersonToKV extends DoFn<String, KV<String,Integer>> {
    @ProcessElement
    public void apply(ProcessContext c){
        String input=c.element();
        String arr[]=input.split(",");
        c.output(KV.of(arr[0],Integer.parseInt(arr[3])));
    }
}
