package groupBy;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

public class SelectedPersonsWithHisTotalSum extends DoFn<KV<String,Iterable<Integer>>,String> {

    @ProcessElement
    public void apply(ProcessContext c){
        String strKey=c.element().getKey();
        Iterable<Integer> strValue=c.element().getValue();

        Integer sum=0;
        for(Integer loop:strValue){
            sum+=loop;
        }
        c.output(strKey+","+sum.toString());
    }
}
