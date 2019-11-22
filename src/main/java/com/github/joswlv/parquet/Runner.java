package com.github.joswlv.parquet;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import com.github.joswlv.parquet.processor.Processor;
import com.github.joswlv.parquet.util.ParquetMetaInfoConverter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Runner {

  private Logger log = LoggerFactory.getLogger(this.getClass());
  private ExecutorService service;

  public void run(String[] args) {
    ParquetMetaInfo metaInfo = ParquetMetaInfoConverter.build(args);
    service = Executors
        .newFixedThreadPool(metaInfo.getConcurrent(),
            new ThreadFactoryBuilder().setNameFormat("parquet-rewrite-thread-%d").build());
    CompletionService<String> cs = new ExecutorCompletionService(service);

    List<String> previousFileList = metaInfo.getPreviousFileList();
    for (String parquetFile : previousFileList) {
      cs.submit(() -> {
        log.info("==> {} Processing Start", parquetFile);
        Processor processor = new Processor(metaInfo, parquetFile);
        processor.process();
        return parquetFile;
      });
    }
    service.shutdown();

    try {
      for (int i = 0; i < previousFileList.size(); i++) {
        log.info("{} Processing Complete!", cs.take().get());
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      service.shutdown();
      throw new RuntimeException(e.getCause());
    }

    try (FileSystem dfs = DistributedFileSystem.newInstance(metaInfo.getConfiguration())) {
      for (String previousFile : previousFileList) {
        if (dfs.delete(new Path(previousFile), true)) {
          log.info("{} - rename ing...", previousFile);
          dfs.rename(new Path(previousFile + "_tmp"), new Path(previousFile));
        }
      }
    } catch (IOException e) {
      log.error("file rename Error! , ", e.getMessage(), e);
    }
  }
}