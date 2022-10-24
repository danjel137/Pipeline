package demo5;

import org.apache.beam.sdk.transforms.DoFn;

public class FilterSedanHatchback extends DoFn<String,String> {
    @ProcessElement
    public void procesElement(ProcessContext c){
        String[]fields=c.element().split(",");
        String body=fields[2];
        if(body.equals("sedan")||body.equals("hatch")){
            c.output(c.element());
        }
    }
}
