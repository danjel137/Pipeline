package creatingAndExecutingPipeline;

import org.apache.beam.sdk.transforms.DoFn;

public class ExtractPaymentTypeFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        String[] tokens = c.element().split(",");
        if (tokens.length >= 4) {
            c.output(tokens[3]);
        }
    }
}
