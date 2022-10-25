package Flatten;

import org.apache.beam.sdk.transforms.DoFn;

public class RemoveHeaderr extends DoFn<String,String> {
    private String header;

    public RemoveHeaderr(String header) {
        this.header = header;
    }
    @ProcessElement
    public void removeHeader(ProcessContext c){
        String st=c.element();
        if(!c.equals(this.header)&&!st.isEmpty()){
            c.output(st);
        }
    }
}
