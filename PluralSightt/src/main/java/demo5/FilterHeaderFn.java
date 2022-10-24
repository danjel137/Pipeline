package demo5;

import org.apache.beam.sdk.transforms.DoFn;

public class FilterHeaderFn extends DoFn<String,String> {

    @ProcessElement
    public void apply(ProcessContext c){
        String text = c.element();
        if(!text.equals(Filtering.CSV_HEADER)&& !text.isEmpty()){
            System.out.println(text);
            c.output(text);
        }
    }
}
