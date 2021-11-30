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
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class PopularNames {
    private static final String PROJECT_ID = "bq-beam-test-project";
    public static final String BUCKET_NAME = "gs://bq-beam-test/";
    public static final Logger LOG = LoggerFactory.getLogger(PopularNames.class);

    public static Pipeline createPipeline(Class<? extends PipelineRunner<?>> runnerClazz) {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        options.setProject(PROJECT_ID);
        options.setTempLocation(BUCKET_NAME + "Temp");
        options.setRunner(runnerClazz);
        options.setRegion("europe-central2");
        options.setJobName("process-avro-to-csv2");
        LOG.info("Will be run with " + options.getRunner().getName());
        return Pipeline.create(options);
    }

    public static PCollection<Birth> readAvro(Pipeline p, String inputFilePath) {
        PCollection<Birth> records =  p.apply("(1) Read input Avro file",
                        AvroIO.read(Birth.class).from(inputFilePath).withBeamSchemas(true));
        LOG.debug("Has input Collections Schema? -> " + records.hasSchema());
        return records;
    }

    public static PCollection<Birth> getMaxNumberRecordsByYear(PCollection<Birth> records) {
        PCollection<KV<Long, Birth>> birthsPerYear =
                records.apply("add key by Year to records",
                        WithKeys.of(Birth::getYear).withKeyType(TypeDescriptors.longs()));

        PCollection<KV<Long, Birth>> combined =
                birthsPerYear.apply("group by Year and aggregate by Max Number",
                        Combine.perKey(
                                Max.of(new BirthComparator())));

        return combined.apply("Extract Values from KV pairs",
                Values.create());
    }

    public static PCollection<String> transformToString(PCollection<Birth> records) {
        return records
                .apply("Transform records to String",
                        MapElements
                                .into(TypeDescriptors.strings())
                                .via(Birth::toStringValues));
    }

    public static void writeToCSV(PCollection<String> records, String outputFilePath) {
        List<String> fieldNames = Arrays
                .stream(Birth.class.getFields())
                .map(Field::getName)
                .collect(Collectors.toList());
        String header = String.join(",", fieldNames);
        records.apply(TextIO
                .write()
                .to(outputFilePath)
                .withHeader(header)
                .withSuffix(".csv")
                .withoutSharding());
    }

    public static void print(PCollection<?> records) {
        records.apply(MapElements.into(TypeDescriptors.strings())
                        .via(rec -> {LOG.info(rec.toString());
                                    return "";}
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
