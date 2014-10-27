import java.util.List;

/**
 * Created by wzwang on 14/10/20.
 */
public class Tweet {
    public String id_str;
    public String created_at;
    public String text;
    public Coordinate coordinates;



    public static class Coordinate {
        String type;
        List<Double> coordinates;

        public String toString() {
            return coordinates.toString();
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("id_str: " + id_str + "\n");
        sb.append("created_at: " + created_at + "\n");
        sb.append("text: " + text + "\n");
        if (coordinates != null)
            sb.append("coordinates: " + coordinates.toString());
        return sb.toString();
    }
}