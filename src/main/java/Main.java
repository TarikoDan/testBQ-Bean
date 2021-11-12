import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.stream.Collectors;

public class Main {
    public static <T> void main(String[] args) throws IOException {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

//        String inputFilePath = "src/main/resources/avrousnames.avro";
        String inputFilePath = "src/main/resources/usnames100.avro";
        String inputFileSchema = "src/main/resources/schema.avsc";
        String outputFilePath = "src/main/resources/output/mostPopularNames";

        // Read GenericRecord's of the given schema from files on GCS
        Schema schema = new Schema.Parser().parse(new File(inputFileSchema));
        String header = schema.getFields().stream()
                .map(field -> field.name()).collect(Collectors.joining(","));

        PCollection<GenericRecord> records =
                p.apply(AvroIO.readGenericRecords(schema)
                        .from(inputFilePath));

        PCollection<KV<String, GenericRecord>> keyedByYear =
                records
                        .apply(WithKeys.of(new SimpleFunction<GenericRecord, String>() {
                            @Override
                            public String apply(GenericRecord s) {
                                return s.get("year").toString();
                        }}));

//        PCollection<KV<String, Iterable<GenericRecord>>> grouped =
//                keyedByYear.apply("group", GroupByKey.<String, GenericRecord>create());
//        PCollection<KV<String, GenericRecord>> groupedValues = grouped.apply(Combine.groupedValues(
//                Max.of(new GenComparator())
//        ));

        PCollection<KV<String, GenericRecord>> combined =
                keyedByYear.apply(Combine.perKey(
                        Max.of(new NumberComparator())));

        PCollection<GenericRecord> mostPopularNames = combined.apply(Values.create());
//        print(mostPopularNames);

        PCollection<String> lines = mostPopularNames.apply(ParDo.of(new ExtractValues()));

        lines.apply(
                        "Write CSV formatted data",
                        TextIO.write().to(outputFilePath)
                                .withoutSharding()
                                .withHeader(header)
                                .withSuffix(".csv"));
//

        p.run().waitUntilFinish();

    }

    private static void print(PCollection<? extends Object> pCollection) {
        pCollection
                .apply(
                "Preview Result",
                MapElements.into(TypeDescriptors.strings())
                        .via(
                                x -> {
                                    System.out.println(x);
                                    return "";
                                }));
    }

    private static class NumberComparator implements Comparator<GenericRecord>, Serializable {

        @Override
        public int compare(GenericRecord o1, GenericRecord o2) {
            int number1 = ((Number) o1.get("number")).intValue();
            int number2 = ((Number) o2.get("number")).intValue();
            return Integer.compare(number1, number2);
        }
    }

    private static class ExtractValues extends DoFn<GenericRecord, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            GenericRecord element = c.element();
            String state = element.get("state").toString();
            String gender = element.get("gender").toString();
            String year = element.get("year").toString();
            String name = element.get("name").toString();
            String number = element.get("number").toString();
            String line = String.join(",", state, gender, year, name, number);
            c.output(line);
        }
    }

}

