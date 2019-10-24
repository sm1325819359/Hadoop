package hadoop.ch03.v17034460210;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import org.apache.log4j.BasicConfigurator;

public class DeleteHDFSFile{
    public static void main(String[] args) throws Exception {
        //自动快速地使用缺省Log4j环境
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        //获取配置信息
        Configuration conf = new Configuration();
        //获取namenode地址
        URI uri = new URI("hdfs://192.168.30.131:8020");
        //获取FileSystem对象
        FileSystem fs = FileSystem.get(uri,conf,"hadoop");
        //在hdfs中文件的路径
        Path path = new Path("/17034460210/test2.txt");
        //删除文件test2.txt
        fs.deleteOnExit(path);
        //关闭流
        fs.close();
        System.out.println("删除文件test.txt成功！");
    }
}
