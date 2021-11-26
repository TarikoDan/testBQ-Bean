import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.Comparator;

public class PopularNames {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        System.out.println(options.getRunner());
        Pipeline p = Pipeline.create(options);

        String inputFilePath = "gs://bq-beam/usnames100.avro";
        String outputFilePath = "gs://bq-beam/output/popularNamesRecords";

        // Read Avro-generated classes from files on GCS
        PCollection<Birth> records =
                p.apply("(1) Read input Avro file",
                        AvroIO.read(Birth.class).from(inputFilePath).withBeamSchemas(true));

        System.out.println("hasSchema? -> " + records.hasSchema());

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
                                    System.out.println(rec);
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
