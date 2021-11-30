import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PopularNamesTest {
    public static final String INPUT_FILE_PATH = "src/main/resources/Input/top100NumbersUsNames.avro";
    private static final Logger LOG = LoggerFactory.getLogger(PopularNamesTest.class);
    final Birth birth1 = new Birth("CA", "M", 1960L, "David", 7922L);
    final Birth birth2 = new Birth("NY", "M", 1960L, "John", 8333L);
    final Birth birth3 = new Birth("NY", "M", 1960L, "Michael", 8938L);

    @Rule
    public TestPipeline p = TestPipeline.create();
    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    @Test
    public void readAvroFileTest() {
        LOG.debug("start testing");

        PCollection<Birth> inputRecords = PopularNames.readAvro(p, INPUT_FILE_PATH)
                .apply(Filter.by(input -> input.getYear() == 1960L));
        PopularNames.print(inputRecords);

        List<Birth> expectedResults = Arrays.asList(birth1, birth2, birth3);

        PAssert.that(inputRecords).containsInAnyOrder(expectedResults);

        p.run();
    }

    @Test
    public void pTransformsTest() {
        PCollection<Birth> inputRecords = PopularNames.readAvro(p, INPUT_FILE_PATH);
        PCollection<Birth> maxNumberRecordsByYear = PopularNames.getMaxNumberRecordsByYear(inputRecords)
                .apply(Filter.by(input -> input.getYear() == 1960L));

        List<Birth> expectedResults = Collections.singletonList(birth3);

        PAssert.that(maxNumberRecordsByYear).containsInAnyOrder(expectedResults);

        p.run();
    }

    @Test
    public void toStringTest() {
        PCollection<Birth> inputRecords = PopularNames.readAvro(p, INPUT_FILE_PATH);
        PCollection<Birth> maxNumberRecordsByYear = PopularNames.getMaxNumberRecordsByYear(inputRecords)
                .apply(Filter.by(input -> input.getYear() == 1960L));
        PCollection<String> toString = PopularNames.transformToString(maxNumberRecordsByYear);

        List<String> expectedResults = Collections.singletonList(birth3.toStringValues());

        PAssert.that(toString).containsInAnyOrder(expectedResults);

        p.run();
    }

}
