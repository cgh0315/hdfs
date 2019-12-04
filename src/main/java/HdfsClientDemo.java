import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClientDemo {

	static Configuration conf = new Configuration();
	static FileSystem fs;

	static {
		try {
			fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) throws Exception {

		RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"),true);
//		FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
//		while(locatedFileStatusRemoteIterator.hasNext()){
//			System.out.println(locatedFileStatusRemoteIterator.next().getPath());
//		}
		// 查找文件夹，并遍历里面的文件
//		digui(new Path("/"));
//		FSDataOutputStream fsDataOutputStream = fs.create(new Path("/aaa/aa.txt"),false);
//		fsDataOutputStream.write("abcefg".getBytes());
//		fsDataOutputStream.close();
//		boolean delete = fs.delete(new Path("/aaa"), false);
//		fs.rename(new Path("/bbb/aa.txt"),new Path("/aa.txt"));
//		boolean delete = fs.mkdirs(new Path("/aaa"));
//		System.out.println(delete);
//		FSDataInputStream open = fs.open(new Path("/aa.txt"));
//		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open, "utf-8"));
//		String line = "";
//		while ((line = bufferedReader.readLine()) != null){
//			System.out.println(line);
//		}
		FSDataOutputStream fsDataOutputStream = fs.create(new Path("/aa.txt"), true);
		fsDataOutputStream.write("hello baby\n".getBytes());
		fsDataOutputStream.write("hello jina\n".getBytes());
		fsDataOutputStream.write("hello sam\n".getBytes());

	}

	public static void digui(Path path){
		FileStatus[] fileStatuses = new FileStatus[0];
		try {
			fileStatuses = fs.listStatus(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (FileStatus f :
				fileStatuses) {
			if (f.isDirectory()) {
				System.out.println("目录" + f.getPath());
				digui(f.getPath());
			}else System.out.println("文件" + f.getPath());
		}
	}
}
