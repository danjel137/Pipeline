package innerJoin;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class IdGenderKV extends DoFn<String, KV<String,String>> {
    @ProcessElement
    public void apply(
            @Element String input,
            OutputReceiver <KV<String,String>>outputReceiver
    ){
        String arr[]=input.split(",");
        outputReceiver.output(KV.of(arr[0],arr[1]));
    }
}
