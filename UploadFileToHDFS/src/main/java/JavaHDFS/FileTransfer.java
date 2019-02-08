package JavaHDFS;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileTransfer {
    public static void main(String args[]) throws Exception{
        ArrayList <String> sourceUrl = new ArrayList<String>();
        sourceUrl.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2");
        sourceUrl.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2");
        sourceUrl.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2");
        sourceUrl.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2");
        sourceUrl.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2");
        sourceUrl.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2");

        String destination = args[0];

        FileDecompressor decompressor = new FileDecompressor();
        FileRemover remover = new FileRemover();


        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(destination), conf);

        int bookCount=1;
        for(String url:sourceUrl) {
            String fileName = "Book"+bookCount;
            bookCount++;
            URL bookUrl = new URL(url);
            URLConnection conn = bookUrl.openConnection();
            InputStream in = conn.getInputStream();
            String outputPath = destination+"/"+fileName+".bz2";
            OutputStream out = fs.create(new Path(outputPath));
            IOUtils.copyBytes(in, out, 4096, true);
            decompressor.decompress(outputPath);
            remover.removeFile(outputPath);
            System.out.println("Success: "+outputPath);
        }
    }
}


