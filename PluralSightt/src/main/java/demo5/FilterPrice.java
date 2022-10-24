package demo5;

import org.apache.beam.sdk.transforms.DoFn;

public class FilterPrice extends DoFn<String,String> {
    private Double priceThreshHold=0.0;

    public FilterPrice(Double priceThreshHold) {
        this.priceThreshHold = priceThreshHold;
    }
    @ProcessElement
    public void apply(@Element String line,OutputReceiver<String>out){
        String[]fields=line.split(",");
        double price=Double.parseDouble(fields[1]);

        if(price!=0&& price<priceThreshHold){
            out.output(line);
        }
    }
}
