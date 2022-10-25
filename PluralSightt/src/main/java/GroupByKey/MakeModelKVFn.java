package GroupByKey;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class MakeModelKVFn extends DoFn<CarModel, KV<String,Double>> {
    @ProcessElement
    public void processElement(ProcessContext c){
        CarModel carModel = c.element();
        c.output(KV.of(carModel.getModel(), carModel.getPrice()));
    }
}
