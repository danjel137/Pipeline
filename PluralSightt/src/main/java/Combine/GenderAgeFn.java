package Combine;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class GenderAgeFn extends DoFn<String, KV<String,Double>> {
    @ProcessElement
    public void apply(
            @Element String element,
            OutputReceiver<KV<String,Double>>outputReceiver){
        String []fields=element.split(",");
        String gender=fields[1];
        double age=Double.parseDouble(fields[2]);
        outputReceiver.output(KV.of(gender,age));
    }
}
