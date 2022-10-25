package Combine;

import org.apache.beam.sdk.transforms.SerializableFunction;


public class AverageFn implements SerializableFunction<Iterable<Double>,Double> {
    @Override
    public Double apply(Iterable<Double>input){
        double sum=0;
        double count=0;
        for (double item: input){
            sum+=item;
            count+=1;
        }

        return sum/count;
    }
}
