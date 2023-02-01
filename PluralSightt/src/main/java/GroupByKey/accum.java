package GroupByKey;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.StatsAccumulator;

public class accum implements Combine.AccumulatingCombineFn.Accumulator {
    double min;


    //@Override
//    public void addInput(CarModel input,Object o) {
//        this.min = Math.min(this.min,input.getPrice());
//    }

//    @Override
//    public void mergeAccumulator(StatsAccumulator other) {
//
//    }

    @Override
    public void addInput(Object input) {
//        CarModel carModel=new CarModel();
//        this.min = Math.min(this.min,input.getPrice());
    }

    @Override
    public void mergeAccumulator(Object other) {

    }

    @Override
    public Object extractOutput() {
        return null;
    }
}

