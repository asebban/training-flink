package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CountingSink implements SinkFunction<String> {

    @Override
    public void invoke(String value, Context context) {
        System.out.println("Received: " + value); // Afficher la valeur reçue et le compteur
        // Écrire dans un fichier
        writeToFile("elements.txt", value);
    }

    private void writeToFile(String fileName, String content) {
        File file = new File(fileName);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
            writer.write(content);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
