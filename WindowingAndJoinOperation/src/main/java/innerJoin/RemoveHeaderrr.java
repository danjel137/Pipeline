package innerJoin;

import org.apache.beam.sdk.transforms.DoFn;

public class RemoveHeaderrr extends DoFn<String,String> {
    private String header;

    public RemoveHeaderrr(String header) {
        this.header = header;
    }
    @ProcessElement
    public void apply(ProcessContext c){
        String st=c.element();
        if(!c.element().equals(this.header)&& (!c.element().isEmpty())){
            c.output(st);
        }
    }
}
