import java.io.IOException;

public class Producer {
    public static void main(String[] args) throws IOException {
        HoseBirdClient client=new HoseBirdClient();
        client.getHoseBirdClient().connect();
    }
}
