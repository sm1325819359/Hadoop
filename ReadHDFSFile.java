package hadoop.ch03.v17034460210;
/**
 *查看文件test5.txt的内容
 * */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

import java.net.URI;
public class ReadHDFSFile {
    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure();
        //获取配置信息
        Configuration conf = new Configuration();
        //获取namenode地址
        URI uri = new URI("hdfs://192.168.30.131:8020");
        //获取FileSystem对象
        FileSystem fs = FileSystem.get(uri,conf,"hadoop");
        //在HDFS中test5的路径
        Path path=new Path("/17034460210/text4.txt");
        FSDataInputStream dis = fs.open(path);
        String str=null;
        while((str=dis.readLine())!=null){
            System.out.println(str);
        }
        //关闭fs流
        fs.close();
    }
}
