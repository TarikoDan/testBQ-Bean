import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PCollection;

public class Main {
    public static void main(String[] args) {
//        Pipeline p = PopularNames.createPipeline(DirectRunner.class);
        Pipeline p = PopularNames.createPipeline(DataflowRunner.class);

        String inputFilePath = PopularNames.BUCKET_NAME + "Input/top100NumbersUsNames.avro";
        String outputFilePath = PopularNames.BUCKET_NAME + "output/mostPopularNames";
//        String inputFilePath = "src/main/resources/" + "Input/top100NumbersUsNames.avro";
//        String outputFilePath = "src/main/resources/" + "output/mostPopularNames";

        // Read Avro-generated classes from files on GCS
        PCollection<Birth> records = PopularNames.readAvro(p, inputFilePath);

        // Group and Aggregate Transforms.
        PCollection<Birth> maxNumberRecordsByYear = PopularNames.getMaxNumberRecordsByYear(records);

        // Transformation to string for the sake of recording.
        PCollection<String> mostPopularNames = PopularNames.transformToString(maxNumberRecordsByYear);

        PopularNames.print(mostPopularNames.apply(Sample.any(10L)));

        PopularNames.writeToCSV(mostPopularNames, outputFilePath);

        p.run().waitUntilFinish();

    }
}
