import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

import java.io.Serializable;
import java.util.Objects;

//@DefaultCoder(AvroCoder.class)
@DefaultSchema(JavaBeanSchema.class)
//@DefaultSchema(JavaFieldSchema.class)
public class Birth implements Serializable {
    public String state;
    public String gender;
    public Long year;
    public String name;
    public Long number;

    public String getState() {
        return state;
    }

    public String getGender() {
        return gender;
    }

    public String getName() {
        return name;
    }

    public Long getNumber() {
        return number;
    }

    public Long getYear() {
        return year;
    }

    public Birth() {
    }

    @SchemaCreate
    public Birth(String state, String gender, Long year, String name, Long number) {
        this.state = state;
        this.gender = gender;
        this.year = year;
        this.name = name;
        this.number = number;
    }

    @Override
    public String toString() {
        return "Birth{" +
                "state='" + state + '\'' +
                ", gender='" + gender + '\'' +
                ", year=" + year +
                ", name='" + name + '\'' +
                ", number=" + number +
                '}';
    }

    public String toStringValues() {
        return state + ',' +
                gender + ',' +
                year + ',' +
                name + ',' +
                number;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Birth birth = (Birth) o;
        return Objects.equals(state, birth.state) && Objects.equals(gender, birth.gender) && Objects.equals(year, birth.year) && Objects.equals(name, birth.name) && Objects.equals(number, birth.number);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, gender, year, name, number);
    }
}
