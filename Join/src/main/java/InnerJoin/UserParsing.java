package InnerJoin;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class UserParsing extends DoFn<String, KV<String,String>> {
    @ProcessElement
    public void apply(ProcessContext c){
        String arr[]=c.element().split(",");
        String strKey=arr[0];
        String strVal=arr[1];
        c.output(KV.of(strKey,strVal));

    }
}
