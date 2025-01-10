package solutions.Infy.kafka.connect.sink;

import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.BufferedReader;
import java.util.Collection;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import solutions.Infy.kafka.connect.Version;


public class HeaderBasedSinkTask extends SinkTask {

    private Writer writer;
    private static final Logger log = LoggerFactory.getLogger(HeaderBasedSinkTask.class);

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info("Starting HeaderBasedSinkTask with properties -.",properties);
        log.info("starting the task with properties in string -", properties.toString());
        initwriter(properties); // Ensure this initializes Writer correctly
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Processing {} records in HeaderBasedSinkTask.", records);
        log.trace("Trace: Putting {} to HeaderBasedSink.", records);
        writer.write(records);

        log.info("Info log after writing the record -{}",records);
    }

    @Override
    public void stop() {
        log.info("Stopping HeaderBasedSinkTask...");
        if(writer != null){
            //writer.stop();
        }
    }

    private void initwriter(final Map<String, String> config) {
        log.info("Initializing properties in Task with config details - {}", config);
        this.writer = new Writer(config);
    }

    @SuppressWarnings("resource")
private String asciiArt(){
return new BufferedReader(
        new InputStreamReader(getClass().getResourceAsStream("/HeaderBasedRouting-Sink-ascii.txt")))
        .lines()
        .collect(Collectors.joining("\n"));
    }
}


