import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class PopularNamesTest {
    @Rule
    public TestPipeline p = TestPipeline.create();

    private static final Logger LOG = LoggerFactory.getLogger(PopularNamesTest.class);

    @Test
    public void testMinimalWordCount() {
        LOG.debug("start testing");
        String inputFilePath = "src/main/resources/top100NumbersUsNames.avro";

        final Birth birth2 = new Birth("CA", "M", 1991L, "Michael", 7573L);

        PCollection<KV<Long, Birth>> inputRecords = p
                .apply("(1) Read input Avro file",
                        AvroIO.read(Birth.class).from(inputFilePath).withBeamSchemas(true))
                .apply(Filter.by(input -> input.getYear() == 1991L))
                .apply("add key by Year to records",
                        WithKeys.of(Birth::getYear).withKeyType(TypeDescriptors.longs()))
                ;

        PopularNames.print(inputRecords);

        List<KV<Long, Birth>> expectedResults = Collections.singletonList(
                KV.of(1991L, birth2));

        PAssert.that(inputRecords).containsInAnyOrder(expectedResults);

        p.run();
    }

}
