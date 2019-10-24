package hadoop.ch03.v17034460210;
/**
 * 将本地上F:/test4.txt上传到HDFS的/17034460241目录中
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.BasicConfigurator;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class UploadHDFSFile {
    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        //获取配置信息
        Configuration conf = new Configuration();
        //获取namenode地址
        URI uri = new URI("hdfs://192.168.30.131:8020");
        //获取FileSystem对象
        FileSystem fs = FileSystem.get(uri,conf,"hadoop");
        //本地文件地址
        Path src=new Path("E:\\text4.txt");
        //上传到HDFS的地址
        Path dst=new Path("/17034460210/text4.txt");
        //利用filesystem的copyFromLocalFile接口
        fs.copyFromLocalFile(src,dst);
        //关闭fs流
        fs.close();

        System.out.println("上传文件到HDFS成功");
    }
}