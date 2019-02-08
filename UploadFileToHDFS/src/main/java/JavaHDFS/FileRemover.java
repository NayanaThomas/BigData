package JavaHDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class FileRemover {
 public void removeFile(String fileLocation)
    {
        Configuration config = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(fileLocation), config);
            fs.delete(new Path(fileLocation), true);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
