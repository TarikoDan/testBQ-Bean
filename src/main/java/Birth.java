import java.util.Objects;

public class Birth {
    String state;
    String gender;
    Integer year;
    String name;
    Integer number;

    public Birth() {
    }

    public Birth(String state, String gender, Integer year, String name, Integer number) {
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
