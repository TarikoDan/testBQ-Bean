import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class Main {
    public static <T> void main(String[] args) throws IOException {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

//        String inputFilePath = "src/main/resources/avrousnames.avro";
        String inputFilePath = "src/main/resources/usnames100.avro";
        String inputFileSchema = "src/main/resources/schema.avsc";
        String outputFilePath = "src/main/resources/output/res";


//        // Read Avro-Schema from file GCS
//        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
//        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(inputFilePath), datumReader);
//        Schema schema = dataFileReader.getSchema();
//        System.out.println(schema); // this will print the schema for you

/*
        // Read Avro-generated classes from files on GCS
        PCollection<Birth> records =
                p.apply("(1) Read input Avro file",
                        AvroIO.read(Birth.class).from(inputFilePath));
*/

        // Read GenericRecord's of the given schema from files on GCS
        Schema schema = new Schema.Parser().parse(new File(inputFileSchema));
        PCollection<GenericRecord> records =
                p.apply(AvroIO.readGenericRecords(schema)
                        .from(inputFilePath));
//
//        PCollection<KV<String, Long>> wordCount = p
//                .apply("(1) Read all lines",
//                        TextIO.read().from(inputFilePath))
//                .apply("(2) Flatmap to a list of words",
//                        FlatMapElements.into(TypeDescriptors.strings())
//                                .via(line -> Arrays.asList(line.split("\\s"))))
//                .apply("(3) Lowercase all",
//                        MapElements.into(TypeDescriptors.strings())
//                                .via(word -> word.toLowerCase()))
//                .apply("(4) Trim punctuations",
//                        MapElements.into(TypeDescriptors.strings())
//                                .via(word -> word.trim()))
//                .apply("(5) Filter stopwords",
//                        Filter.by(word -> !isStopWord(word)))
//                .apply("(6) Count words",
//                        Count.perElement());
//
//        wordCount.apply(MapElements.into(TypeDescriptors.strings())
//                        .via(count -> count.getKey() + " --> " + count.getValue()))
//                .apply(TextIO.write().to(outputFilePath));
//
//        p.run().waitUntilFinish();

//        records.apply(ParDo.of(new DoFn<GenericRecord, String>() {
//                            @ProcessElement
//                            public void processElement(@Element ProcessContext c) {
//                                GenericRecord name = c.element();
//                                c.output(name.toString());
//                                System.out.println(name);
//                            }
//                        })
//                );


        PCollection<GenericRecord> filtered = records
                .apply(Filter.by(input -> input.get("year").toString().equals("1954")));


        PCollection<KV<String, GenericRecord>> trans =
                records
//                        .apply(Filter.by(input -> input.get("state").toString().equals("AL")))
                        .apply(WithKeys.of(new SimpleFunction<GenericRecord, String>() {
                            @Override
                            public String apply(GenericRecord s) {
                                return s.get("year").toString();
                        }}));

        PCollection<KV<String, GenericRecord>> combined =
                trans.apply(Combine.perKey(
                        Min.of(new GenComparator())));

        PCollection<GenericRecord> res = combined.apply(Values.create());

//        PCollection<KV<String, Iterable<GenericRecord>>> grouped =
//                trans.apply("group", GroupByKey.<String, GenericRecord>create());
//        PCollection<KV<String, GenericRecord>> groupedValues = grouped.apply(Combine.groupedValues(
//                Max.of(new GenComparator())
//        ));


        res
                .apply(
                "Preview Result",
                MapElements.into(TypeDescriptors.strings())
                        .via(
                                x -> {
                                    System.out.println(x);
                                    return "";
                                }));

        p.run().waitUntilFinish();

    }

    static class GenComparator implements Comparator<GenericRecord>, Serializable {

        @Override
        public int compare(GenericRecord o1, GenericRecord o2) {
            int number1 = ((Number) o1.get("number")).intValue();
            int number2 = ((Number) o2.get("number")).intValue();
            return Integer.compare(number2, number1);
        }
    }

}
