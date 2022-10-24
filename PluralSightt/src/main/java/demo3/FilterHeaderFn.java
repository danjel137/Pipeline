package demo3;

import org.apache.beam.sdk.transforms.DoFn;

public class FilterHeaderFn extends DoFn<String, String> {
    private final String header;


    public FilterHeaderFn(String header) {
        this.header = header;
    }

    @ProcessElement
    public void apply(ProcessContext c){
        String row=c.element();

        if(!row.isEmpty() && !row.equals(header)){
            c.output(row);
        }
    }
}
