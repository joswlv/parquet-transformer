package com.github.joswlv.parquet;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import com.github.joswlv.parquet.processor.Processor;
import com.github.joswlv.parquet.util.ParquetMetaInfoConverter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Runner {

  public void run(String[] args) {
    ParquetMetaInfo metaInfo = ParquetMetaInfoConverter.build(args);
    ExecutorService service = Executors
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
        log.info("{} Processiong Complete!", cs.take().get());
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      service.shutdown();
      throw new RuntimeException(e.getCause());
    }
  }
}
