package innerJoin;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class IdScoreKV extends DoFn<String, KV<String,Integer>> {
    @ProcessElement
    public void apply(
            @Element String inp,
            OutputReceiver <KV<String,Integer>>out
    )
    {
        String []arr=inp.split(",");

        out.output(KV.of(arr[0],Integer.parseInt(arr[1])));
    }
}
