package accumulator;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.math.StatsAccumulator;

//public class SumCollector implements Combine.AccumulatingCombineFn.Accumulator {
//        double min;
//        double max;
//        double sum;
//        int count;
//
//
//
//    @Override
//    public void addInput(Object info) {
//        this.min = Math.min(this.min, info.rating);
//        this.max = Math.max(this.max, info.rating);
//        this.sum += info.rating;
//        this.count++;
//    }
//
//    @Override
//    public void mergeAccumulator(StatsAccumulator other) {
//        this.min = Math.min(this.min, other.min);
//        this.max = Math.max(this.max, other.max);
//        this.sum += other.sum;
//        this.count += other.count;
//    }
//
//    @Override
//    public Object extractOutput() {
//        return null;
//    }
//}
