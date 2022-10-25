package Combine;

import org.apache.beam.sdk.transforms.DoFn;

public class ExtractAgeFn extends DoFn<String,Double> {
    @ProcessElement
    public void apply(
            @Element String element,
            OutputReceiver <Double>out){
        String[]fields=element.split(",");
        double age=Double.parseDouble(fields[2]);

        out.output(age);
    }
}
