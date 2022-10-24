package sideInput;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

public class SideInputExample {
    public static void main(String[] args) {
        Pipeline pipeline=Pipeline.create();

        PCollection<KV<String,String>> pReturn =pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\sideInputs1Input.csv"))
                .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void apply(ProcessContext c){
                        String arr[]=c.element().split(",");
                        c.output(KV.of(arr[0],arr[1]));
                    }
                }));

        PCollectionView<Map<String,String>>pMap=pReturn.apply(View.asMap());

        PCollection<String>pCustList=pipeline.apply(TextIO.read().from("C:\\Users\\HP\\IdeaProjects\\Pipelines\\src\\main\\resources\\sideInputs2Input.csv"));

        pCustList.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void process(ProcessContext c){
                Map<String,String>pInsideView=c.sideInput(pMap);

                String arr[]=c.element().split(",");
                String custName=pInsideView.get(arr[0]);
                System.out.println(custName);

            }
        }).withSideInputs(pMap));
        pipeline.run();
        }



    }

