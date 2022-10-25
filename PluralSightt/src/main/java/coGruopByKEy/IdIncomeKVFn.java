package coGruopByKEy;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class IdIncomeKVFn extends DoFn<String, KV<String,Integer>> {
    @ProcessElement
    public void apply(@Element String element,OutputReceiver<KV<String,Integer>>out){
        String []fields=element.split(",");
        String id=fields[0];
        int income=Integer.parseInt(fields[3]);

        out.output(KV.of(id,income));
    }
}
