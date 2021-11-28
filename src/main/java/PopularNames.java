import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Comparator;

public class PopularNames {
    private static final String PROJECT_ID = "bq-beam-test-project";
    private static final String BUCKET_NAME = "gs://bq-beam-main/";
    public static final Logger LOG = LoggerFactory.getLogger(PopularNames.class);

    public static void main(String[] args) {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
//        PipelineOptions o = PipelineOptionsFactory.create();

        options.setProject(PROJECT_ID);
        options.setTempLocation(BUCKET_NAME + "Temp");
        options.setRunner(DataflowRunner.class);
//        options.setRunner(DirectRunner.class);
        options.setRegion("europe-central2");
        options.setJobName("process-avro-to-csv2");
        LOG.info("Will be run with " + options.getRunner().getName());


        Pipeline p = Pipeline.create(options);

        String inputFilePath = BUCKET_NAME + "Input/top100NumbersUsNames.avro";
        String outputFilePath = BUCKET_NAME + "output/popularNamesRecords";

        // Read Avro-generated classes from files on GCS
        PCollection<Birth> records =
                p.apply("(1) Read input Avro file",
                        AvroIO.read(Birth.class).from(inputFilePath).withBeamSchemas(true));

        System.out.println("Has input Collections Schema? -> " + records.hasSchema());

        PCollection<KV<Long, Birth>> birthsPerYear =
                records.apply("add key by Year to records",
                                WithKeys.of(Birth::getYear).withKeyType(TypeDescriptors.longs()));

        PCollection<KV<Long, Birth>> combined =
                birthsPerYear.apply("group by Year and aggregate by Max Number",
                        Combine.perKey(
                        Max.of(new BirthComparator())));

        PCollection<Birth> popularNamesRecords = combined.apply(Values.create());

        PCollection<String> mostPopularNames = popularNamesRecords
                .apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via(Birth::toStringValues));

        print(mostPopularNames.apply(Sample.any(10L)));

        String header = String.join(",", popularNamesRecords.getSchema().getFieldNames());

        mostPopularNames.apply(TextIO
                .write()
                .to(outputFilePath)
                .withHeader(header)
                .withSuffix(".csv")
                .withoutSharding()
        );

        p.run().waitUntilFinish();

    }

    public static void print(PCollection<?> records) {
        records.apply("Preview Result",
                MapElements.into(TypeDescriptors.strings())
                        .via(rec -> {
                                    LOG.info(rec.toString());
                                    return "";
                                }
                        ));
    }

    static class BirthComparator implements Comparator<Birth>, Serializable {

        @Override
        public int compare(Birth o1, Birth o2) {
            int number1 = (o1.getNumber()).intValue();
            int number2 = (o2.getNumber()).intValue();
            return Integer.compare(number1, number2);
        }
    }

}
