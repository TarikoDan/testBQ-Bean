import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
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
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws IOException {
        Birth birth = new Birth("Flor", "M", 1992, "name", 12);
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        String inputFilePath = "src/main/resources/avrousnames.avro";
        String inputFileSchema = "src/main/resources/schema.avsc";
        String outputFilePath = "src/main/resources/output/res";

//        // Read Avro-Schema from file GCS
//        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
//        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(inputFilePath), datumReader);
//        Schema schema = dataFileReader.getSchema();
//        System.out.println(schema); // this will print the schema for you


        // Read Avro-generated classes from files on GCS
        PCollection<Birth> records =
                p.apply("(1) Read input Avro file",
                        AvroIO.read(Birth.class).from(inputFilePath));


        // Read GenericRecord's of the given schema from files on GCS
//        Schema schema = new Schema.Parser().parse(new File(inputFileSchema));
//        PCollection<GenericRecord> records =
//                p.apply(AvroIO.readGenericRecords(schema)
//                        .from(inputFilePath));
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

        records.apply(ParDo.of(new DoFn<Birth, String>() {
                            @ProcessElement
                            public void processElement(@Element ProcessContext c) {
                                Birth name = c.element();
                                c.output(name.toString());
                                System.out.println(name);
                            }
                        })
                );


        p.run().waitUntilFinish();

    }

    private static boolean isStopWord(String word) {
        return false;
    }
}
