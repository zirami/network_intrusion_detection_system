import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.*;

import static com.sun.jmx.mbeanserver.Util.cast;
import static java.nio.file.StandardWatchEventKinds.*;

public class FileReadProducer{

    final Logger logger = LoggerFactory.getLogger(FileReadProducer.class);
    private WatchService watcher;
    private Map<WatchKey,Path> keys;

    public static void main(String[] args) { new FileReadProducer().run();}
    public FileReadProducer(){}

    public void run(){
        //Tạo luồng mạng
        final KafkaProducer<String, String> producer = createKafkaProducer();
        String filePath = "";
        String topic = "netflows";

        //Shutdown hook 
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Shutting down producer");
            producer.flush();
            producer.close();
//            try {
//                watcher.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
            logger.info("Done!");
        }));

        Path inputPath = Paths.get(filePath);
        if (Files.isDirectory(inputPath)){
            //theo doi thu muc de gui file moi
            watchDirectory(producer, topic, inputPath);

        }else {
            //gui single file
            openAndSendSingleFile(producer, topic, inputPath);
        }
    }

    public void watchDirectory(KafkaProducer<String, String> producer, String topic, Path inputPath){
        //Dang ky mot thu muc de giam sat
        try {
            watcher = FileSystems.getDefault().newWatchService();
            keys = new HashMap<WatchKey,Path>();
            WatchKey key = inputPath.register(watcher, ENTRY_CREATE, ENTRY_MODIFY);
            keys.put(key, inputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Bo phieu co cac su kien trong thu muc
        while(true){
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }

            for (WatchEvent<?> event: key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                System.out.println(kind.toString());
                if (kind == OVERFLOW) {
                    continue;
                }

                // ten file la ngu canh cua su kien
                WatchEvent<Path> ev = cast(event);
                Path filename = ev.context();
                System.out.println(filename);

                //Gui file moi
                openAndSendSingleFile(producer, topic, filename);
            }
            boolean valid = key.reset();
            if (!valid) break;

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void openAndSendSingleFile(KafkaProducer<String, String> producer, String topic, Path inputPath){

        //kiem tra input la text document
        Tika tika = new Tika();
        try{System.out.println(tika.detect(inputPath).toString());
            if (!tika.detect(inputPath).matches("text/plain")){
                throw new IllegalArgumentException();
            }
        }catch (IOException e){
            e.printStackTrace();
        }catch (IllegalArgumentException e){
            logger.error("Input: " + inputPath.toString() + "\n" + " is not a text document!");
        }

        //Mo file input va send data
        try(Stream<String> lines = Files.lines(inputPath)){
            lines.forEach(line -> {
                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(topic, line);

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            // exception is thrown
                            logger.error("Error while producing", e);
                        } else {
                            logger.info("Offset: " +recordMetadata.offset());
                        }
                    }
                });

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public KafkaProducer<String, String> createKafkaProducer(){

        String bootstrapServers = "127.0.0.1:9092";
        //Tao cac thuoc tinh cho Producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        //Tao Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        return producer;
    }
}