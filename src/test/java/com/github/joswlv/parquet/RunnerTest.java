package com.github.joswlv.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import com.github.joswlv.parquet.util.ParquetMetaInfoConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RunnerTest extends TestHdfsBuilder {

  private ParquetMetaInfo parquetMetaInfo;
  private String requiredArg;

  @BeforeEach
  void Before() throws IOException {
    requiredArg = "--table-name=edw.sample --key-col-name=id --key-col-value=99;100 --target-col-name=email;gender --concurrent=1 --default-fs=hdfs://localhost:11000";
    //--skip-file-option=true";
    parquetMetaInfo = ParquetMetaInfoConverter.build(requiredArg.split(" ", -1));
  }

  /**
   * process 종료 후처리가 잘 되는가?
   */
  @Test
  void 종료_후처리_TEST() throws IOException {
    List<String> beforeFilePath = getAllFilePath(new Path(parquetMetaInfo.getOriginSourcePath()),
        dfs);
    System.out.println("==Before==");
    beforeFilePath.forEach(System.out::println);
    Runner runner = new Runner();
    runner.run(requiredArg.split(" ", -1));

    List<String> afterFilePath = getAllFilePath(new Path(parquetMetaInfo.getOriginSourcePath()),
        dfs);
    System.out.println("==After==");
    afterFilePath.forEach(System.out::println);

    assertEquals(beforeFilePath, afterFilePath);
  }


  private static List<String> getAllFilePath(Path filePath, FileSystem fs) throws IOException {
    List<String> fileList = new ArrayList<>();
    FileStatus[] fileStatus = fs.listStatus(filePath);
    for (FileStatus fileStat : fileStatus) {
      if (fileStat.isDirectory()) {
        fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
      } else {
        fileList.add(fileStat.getPath().toString());
      }
    }
    return fileList;
  }
}