package parDo;

import org.apache.beam.sdk.transforms.DoFn;

public class Userr extends DoFn<String,String> {
    @ProcessElement
    public void apply(ProcessContext c){
        String line=c.element();
        assert line != null;
        String[] arr =line.split(",");
        if(arr[3].equals("Los Angelos")){
            c.output(line);
        }
    }

}
