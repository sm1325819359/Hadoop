package hadoop.ch03.v17034460210;
/**
 *将test5.txt从hdfs下载到本地
 * */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;
import org.apache.log4j.BasicConfigurator;
public class DownloadHDFSFile {
    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure();
        //获取配置信息
        Configuration conf = new Configuration();
        //获取namenode地址
        URI uri = new URI("hdfs://192.168.30.131:8020");
        //获取FileSystem对象
        FileSystem fs = FileSystem.get(uri,conf,"hadoop");
        Path src=new Path("/17034460210/text4.txt");
        //创建test5.txt并输入“Hello Word”
        Path dst = new Path("E:\\text4.txt");
        FSDataOutputStream os = fs.create(dst,true);
        os.writeBytes("Hello Myworld");
        //关闭输出流
        os.close();

        //linux中
        fs.copyToLocalFile(src,dst);
        //windows中
        fs.copyToLocalFile(false,dst,src,true);
        //关闭fs流
        fs.close();
        System.out.println("下载文件成功");
    }
}
