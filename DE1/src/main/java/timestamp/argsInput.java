package timestamp;

import jdk.jfr.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface argsInput extends PipelineOptions {
@Description("inputi")
    String getInputFilePattern();

void setInputFilePattern(String input);

@Description("autp+uti")
    String getOutputFilePattern();
    void setOutputFilePattern(String output);
}
