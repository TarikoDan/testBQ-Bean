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

public class Main {
    public static void main(String[] args) throws IOException {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        String inputFilePath = "src/main/resources/usnames10.avro";
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

//        records.apply(ParDo.of(new DoFn<Birth, String>() {
//                            @ProcessElement
//                            public void processElement(@Element ProcessContext c) {
//                                Birth name = c.element();
//                                c.output(name.toString());
//                                System.out.println(name);
//                            }
//                        }));
        records.apply(MapElements.via(
                new SimpleFunction<Birth, String>() {
                    @Override
                    public String apply(Birth line) {
                        System.out.println(line);
                        return "";
                    }
                }));


        p.run().waitUntilFinish();

    }

}
