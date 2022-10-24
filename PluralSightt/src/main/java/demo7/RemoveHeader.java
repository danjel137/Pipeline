package demo7;

import org.apache.beam.sdk.transforms.DoFn;

public class RemoveHeader extends DoFn<String,String> {
    private final String header;

    public RemoveHeader(String header) {
        this.header = header;
    }

    @ProcessElement
    public void apply(ProcessContext c){
        String st=c.element();
        if((!st.equals(this.header)&& (!st.isEmpty()))){
            c.output(st);
        }
    }
}
