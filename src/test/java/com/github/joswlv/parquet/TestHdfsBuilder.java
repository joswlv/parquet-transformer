package com.github.joswlv.parquet;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class TestHdfsBuilder {

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
    dfs = miniDFSCluster.getFileSystem();

    Path testDirPath = new Path(hdfsUri + "/test");
    Path sourcePath = new Path(getClass().getClassLoader().getResource("data.0.parq").toURI());
    dfs.copyFromLocalFile(sourcePath, testDirPath);
  }

  @AfterAll
  public void tearDown() {
    miniDFSCluster.shutdown();
    FileUtil.fullyDelete(testDir);
  }
}
