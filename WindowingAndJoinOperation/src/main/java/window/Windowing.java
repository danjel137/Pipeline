package window;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;


public class Windowing {
    public static void main(String[] args) {
        PipelineOptions options= PipelineOptionsFactory.create();
        Pipeline pipeline=Pipeline.create(options);
        PCollection<MovieTag> moviesTag=pipeline
                .apply("ReadMovieTags", TextIO.read()
                        .from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\WindowingAndJoinOperation\\src\\main\\resources\\tags.csv"))
                .apply("ParseMovieTage",ParDo.of(new ParseMovieTag()))
                .apply( "TimeStamp", WithTimestamps.of(MovieTag::getTimestamp));
//                .apply(ParDo.of(new DoFn<MovieTag, String>() {
//                    @ProcessElement
//                    public  void ada(ProcessContext context){
//                        System.out.println(context.element().asCsvRow(","));
//                    }
//                }));

         moviesTag.apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(20))))
                 .apply("ExtractTags", MapElements
                         .into(TypeDescriptors.strings())
                         .via(MovieTag::getTag))
                 .apply(Count.perElement())
                 .apply("ExtractTags",MapElements
                         .into(TypeDescriptors.strings())
                         .via(kv->kv.getKey()+", "+kv.getValue()))
                 .apply("WriteToFile",TextIO.write().
                         to("C:\\Users\\HP\\IdeaProjects\\Pipelines\\WindowingAndJoinOperation\\src\\main\\resources\\tagswindow.csv")
                         .withNumShards(1)
                         .withWindowedWrites());
        pipeline.run();

    }
}
