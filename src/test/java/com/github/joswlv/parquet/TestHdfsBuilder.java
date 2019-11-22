package com.github.joswlv.parquet;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class TestHdfsBuilder {

  private final static String BIGDATA_HDFS_PREFIX = "/user/hive/warehouse/";
  private final static String BIGDATA_TABLE_PREFIX = "edw.db/sample";

  private File testDir;
  private MiniDFSCluster miniDFSCluster;
  protected DistributedFileSystem dfs;
  protected String hdfsUri;
  protected Configuration conf;

  @BeforeAll
  public void setUp() throws Exception {
    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      File winutilsFile = new File(
          getClass().getClassLoader().getResource("hadoop/bin/winutils.exe").toURI());
      System.setProperty("hadoop.home.dir",
          winutilsFile.getParentFile().getParentFile().getAbsolutePath());
      System.setProperty("java.library.path", winutilsFile.getParentFile().getAbsolutePath());
      Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
      fieldSysPath.setAccessible(true);
      fieldSysPath.set(null, null);
    }

    testDir = Files.createTempDirectory("test_hdfs").toFile();
    conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDir.getAbsolutePath());

    miniDFSCluster = new MiniDFSCluster.Builder(conf).nameNodePort(11000).build();
    hdfsUri = miniDFSCluster.getURI().toString();

    conf.set("fs.default.name", hdfsUri);

    dfs = miniDFSCluster.getFileSystem();
    dfs.setConf(conf);

    Path testDirPath = new Path(hdfsUri + BIGDATA_HDFS_PREFIX + BIGDATA_TABLE_PREFIX);
    Path testDirSubath = new Path(
        hdfsUri + BIGDATA_HDFS_PREFIX + BIGDATA_TABLE_PREFIX + "/dt=20191111");
    dfs.mkdirs(testDirPath);
    dfs.mkdirs(testDirSubath);

    for (int i = 1; i < 3; i++) {
      Path sourcePath = new Path("edw.db/userdata" + i + ".parquet");
      dfs.copyFromLocalFile(sourcePath, testDirPath);
    }

    for (int i = 3; i < 6; i++) {
      Path sourcePath = new Path("edw.db/userdata" + i + ".parquet");
      dfs.copyFromLocalFile(sourcePath, testDirSubath);
    }

    FileStatus[] fileStatuses = dfs.listStatus(testDirPath);
    System.out.println("SIZE : " + fileStatuses.length);
    for (FileStatus fileStatus : fileStatuses) {
      System.out.println(fileStatus.isFile());
      System.out.println(fileStatus.getPath().toString());
    }

    FileStatus[] fileStatuses2 = dfs.listStatus(testDirSubath);
    System.out.println("SIZE SUB : " + fileStatuses2.length);
    for (FileStatus fileStatus : fileStatuses2) {
      System.out.println(fileStatus.isFile());
      System.out.println(fileStatus.getPath().toString());
    }

  }

  @AfterAll
  public void tearDown() {
    miniDFSCluster.shutdown();
    FileUtil.fullyDelete(testDir);
  }
}
