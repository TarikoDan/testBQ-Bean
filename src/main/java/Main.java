import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class Main {
    public static void main(String[] args) {
        Birth birth = new Birth("Flor", "M", 1992, "name", 12);
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
    }
}
