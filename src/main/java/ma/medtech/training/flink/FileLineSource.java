package ma.medtech.training.flink;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class FileLineSource implements SourceFunction<String> {
    private final String filePath;
    private volatile boolean isRunning = true;

    public FileLineSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        InputStream inputStream = FileLineSource.class.getClassLoader().getResourceAsStream(filePath);
        if (inputStream == null) {
            throw new RuntimeException("File not found: " + filePath);
        }


        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;

        while (isRunning && (line = reader.readLine()) != null) {
            ctx.collect(line); // Émet la ligne dans le stream
            Thread.sleep(500); // simulation d'une lecture lente
        }

        reader.close();
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
