import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;

public class YouTubeData {
    public void run() throws IOException, InterruptedException, ParserConfigurationException, SQLException, XMLStreamException {
        System.out.println("Введите id каналов");
        Scanner scanner = new Scanner(System.in);
        String [] request = scanner.nextLine().split(" ");
        YouTubeConnection data = new YouTubeConnection(request); //отправляем запрос на сервер
        Constructor constructor = new Constructor();
        constructor.run(data); //заносим данные в БД
    }
}
