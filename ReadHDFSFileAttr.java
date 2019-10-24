package hadoop.ch03.v17034460210;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 *读取文件test5.txt的文件属性
 * */
public class ReadHDFSFileAttr {
    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境
        //获取配置信息
        Configuration conf = new Configuration();
        //获取namenode地址
        URI uri = new URI("hdfs://192.168.30.131:8020");
        //获取FileSystem对象
        FileSystem fs = FileSystem.get(uri,conf,"hadoop");
        //test5的路径
        Path path=new Path("/17034460210/text4.txt");
        //获取文件状态
        FileStatus filestatus=fs.getFileStatus(path);
        //获取数据块大小
        long blockSize=filestatus.getBlockSize();
        System.out.println("数据块大小(blockSize):"+blockSize);

        //获取文件大小
        long fileSize=filestatus.getLen();
        System.out.println("文件大小(fileSize):"+fileSize);

        //获取文件拥有的用户
        String fileOwner=filestatus.getOwner();
        System.out.println("文件拥有用户(fileOwner):"+fileOwner);

        //获取最近访问时间
        SimpleDateFormat sdf=new SimpleDateFormat("yyy-mm-dd hh:mm:ss");
        long accessTime=filestatus.getAccessTime();
        System.out.println("最近访问时间(accessTime):"+sdf.format(new Date(accessTime)));

        //获取最后修改时间
        long modifyTome=filestatus.getModificationTime();
        System.out.println("最后修改时间(modifyTome):"+sdf.format(new Date(modifyTome)));

        //关闭fs
        fs.close();

    }
}
