package coGruopByKEy;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class IdScoreKVFn extends DoFn<String, KV<String,Integer>> {
    @ProcessElement
    public void procesElement(
            @Element String element,
            OutputReceiver<KV<String,Integer>>out){
        String[]fields=element.split(",");
        String id=fields[0];
        int score=Integer.parseInt(fields[1]);
     out.output(KV.of(id,score));
    }
}
