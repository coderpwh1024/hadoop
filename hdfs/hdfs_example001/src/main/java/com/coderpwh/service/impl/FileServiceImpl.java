package com.coderpwh.service.impl;

import com.coderpwh.service.FileService;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.springframework.stereotype.Service;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

import org.apache.hadoop.fs.*;

/**
 * @author coderpwh
 */
@Service
public class FileServiceImpl implements FileService, PathFilter {


    private String reg=".*\\\\.abc";

    //待合并的文件所在的目录的路径
    Path inputPath =  new Path("hdfs://192.168.31.101:9000/user/hadoop/");

    //输出文件的路径
    Path outputPath =  new Path("hdfs://192.168.31.101:9000/user/hadoop/merge.txt");


    /***
     * 合并文件
     * @return
     */
    @Override
    public String mergeFile() {

        reg = ".*\\\\.abc";
        inputPath = new Path("hdfs://192.168.31.101:9000/user/hadoop/a/");
        outputPath = new Path("hdfs://192.168.31.101:9000/user/hadoop/a/merge.txt");
        try {
            doMerge(reg);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "success";
    }


    @Override
    public boolean accept(Path path) {
        if (!(path.toString().matches(reg))) {
            return true;
        }
        return false;
    }

    public void doMerge(String regText) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.31.101:9000");

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        System.out.println("url:"+inputPath.toString());
        System.out.println(URI.create(inputPath.toString()));
        FileSystem fsSource =
                FileSystem.get(URI.create(inputPath.toString()), conf);
        FileSystem fsDst = FileSystem.get(URI.create(outputPath.toString()), conf);
//        FileStatus[] sourceStatus = fsSource.listStatus(inputPath, new MyPathFilter(".*\\.abc"));
        FileStatus[] sourceStatus = fsSource.listStatus(inputPath);
        FSDataOutputStream fsdos = fsDst.create(outputPath);
        PrintStream ps = new PrintStream(System.out);
        for (FileStatus sta : sourceStatus) {
            System.out.print("路径：" + sta.getPath() + " 文件大小：" + sta.getLen() + " 权限：" + sta.getPermission() + " 内容");
            FSDataInputStream fsdis = fsSource.open(sta.getPath());
            byte[] data = new byte[1024];
            int read = -1;
            while ((read = fsdis.read(data)) > 0) {
                ps.write(data, 0, read);
                fsdos.write(data, 0, read);
            }
            fsdis.close();
        }
        ps.close();
        fsdos.close();
    }


}
