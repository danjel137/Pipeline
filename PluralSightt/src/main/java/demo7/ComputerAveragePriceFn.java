package demo7;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ComputerAveragePriceFn extends DoFn<KV<String,Iterable<Double>>,KV<String,Double>>{
    @ProcessElement
    public void processElemet(
            @Element KV<String,Iterable<Double>>element,
            OutputReceiver<KV<String,Double>>out){
        String make=element.getKey();
        int count=0;
        double sumPrice=0;

        for (Double price:element.getValue()){
            sumPrice+=price;
            count++;
        }
        out.output(KV.of(make,sumPrice/count));
    }
}
