package com.github.joswlv.parquet.util;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
import com.github.joswlv.parquet.metadata.ParquetMetaInfoBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetMetaInfoConverter {

  private static Logger log = LoggerFactory.getLogger(ParquetMetaInfoConverter.class);

  private final static String BIGDATA_HDFS_PREFIX = "/user/hive/warehouse/";
  private static ParquetMetaInfo parquetMetaInfo;

  private static Options makeOptions() {
    Options options = new Options();
    options.addOption(Option.builder().longOpt("table-name")
        .desc("the full name of target table [ example : DB.TABLE ]")
        .hasArg()
        .required(true)
        .build());

    options.addOption(Option.builder().longOpt("key-col-name")
        .desc("key column name of target table [ example :  cstno ]")
        .hasArg()
        .required(true)
        .build());

    options.addOption(Option.builder().longOpt("key-col-value")
        .desc(
            "key column value list of target table, delimiter is ';' [ example : 12345;45678;01235 ]")
        .hasArg()
        .required(true)
        .build());

    options.addOption(Option.builder().longOpt("target-col-name")
        .desc("deleted column name list of target table, delimiter is ';'")
        .hasArg()
        .required(true)
        .build());

    options.addOption(Option.builder().longOpt("concurrent")
        .desc("Set multi-thread pool size. Default value count of file.")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("skip-file-option")
        .desc("Set skip file option")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("default-fs")
        .desc("Set default FS")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("source-path")
        .desc("target parquet file dir path")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("compression")
        .desc("compression codec name of new parquet file, when not input impala default value")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("block-size")
        .desc("block size of new parquet file, when not input impala default value")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("page-size")
        .desc("page size of new parquet file, when not input impala default value")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("dict-page-size")
        .desc("dictionary page Size of new parquet file, when not input impala default value")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("writer-version")
        .desc(
            "new parquet file writer version, when not input impala default value [ example : PARQUET_1_0, PARQUET_2_0]")
        .hasArg()
        .build());

    return options;
  }

  public static ParquetMetaInfo build(String[] args) {
    Options options = makeOptions();

    CommandLineParser parser = new DefaultParser();
    CommandLine cl = null;
    try {
      cl = parser.parse(options, args);
    } catch (ParseException e) {
      log.error("arg parsing error!  your args => {} ", args.toString());
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp("help", options);
      System.exit(1);
    }

    Configuration conf = new Configuration();
    parquetMetaInfo = ParquetMetaInfoBuilder.builder()
        .tableFullName(cl.getOptionValue("table-name"))
        .keyColName(cl.getOptionValue("key-col-name"))
        .keyColValueList(Arrays.asList(cl.getOptionValue("key-col-value").split(";")))
        .targetColNameList(Arrays.asList(cl.getOptionValue("target-col-name").split(";")))
        .configuration(conf)
        .build();

    if (cl.hasOption("skip-file-option")) {
      parquetMetaInfo.setSkipFileOption(true);
    }

    if (cl.hasOption("default-fs")) {
      conf.set("fs.defaultFS", cl.getOptionValue("default-fs"));
      parquetMetaInfo.setConfiguration(conf);
    }

    if (cl.hasOption("source-path")) {
      parquetMetaInfo.setOriginSourcePath(cl.getOptionValue("source-path"));
    }

    if (cl.hasOption("compression")) {
      switch (cl.getOptionValue("compression")) {
        case "SNAPPY":
          parquetMetaInfo
              .setRewriteDefaultCompressionCodecName(CompressionCodecName.SNAPPY);
          break;
        case "LZO":
          parquetMetaInfo.setRewriteDefaultCompressionCodecName(CompressionCodecName.LZO);
          break;
        case "GZIP":
          parquetMetaInfo
              .setRewriteDefaultCompressionCodecName(CompressionCodecName.GZIP);
          break;
        case "UNCOMPRESSED":
          parquetMetaInfo
              .setRewriteDefaultCompressionCodecName(CompressionCodecName.UNCOMPRESSED);
          break;
      }
    }

    if (cl.hasOption("block-size")) {
      parquetMetaInfo.setRewriteDefaultBlockSize(
          Integer.parseInt(cl.getOptionValue("block-size")));
    }

    if (cl.hasOption("page-size")) {
      parquetMetaInfo.setRewriteDefaultPageSize(
          Integer.parseInt(cl.getOptionValue("page-size")));
    }

    if (cl.hasOption("dict-page-size")) {
      parquetMetaInfo.setRewriteDefaultDictionaryPageSize(Integer
          .parseInt(cl.getOptionValue("dict-page-size")));
    }

    if (cl.hasOption("writer-version")) {
      switch (cl.getOptionValue("writer-version")) {
        case "PARQUET_1_0":
          parquetMetaInfo.setRewriteParquetFileWriterVersion(
              ParquetProperties.WriterVersion.PARQUET_1_0);
          break;
        case "PARQUET_2_0":
          parquetMetaInfo.setRewriteParquetFileWriterVersion(
              ParquetProperties.WriterVersion.PARQUET_2_0);
          break;
      }
    }

    setPreviousFileList();
    if (cl.hasOption("concurrent")) {
      parquetMetaInfo.setConcurrent(Integer.parseInt(cl.getOptionValue("concurrent")));
    }

    setSchema();
    return parquetMetaInfo;
  }

  private static void setPreviousFileList() {

    ParquetMetaInfoChecker parquetMetaInfoChecker = new ParquetMetaInfoChecker(parquetMetaInfo);

    FileSystem fs;
    String[] sp = parquetMetaInfo.getTableFullName().split("\\.");
    String DB = sp[0].toLowerCase();
    String TABLE = sp[1].toLowerCase();
    List<String> parquetFilePath = new ArrayList<>();

    if (parquetMetaInfo.getOriginSourcePath() == null
        || parquetMetaInfo.getOriginSourcePath().length() == 0) {
      parquetMetaInfo.setOriginSourcePath(BIGDATA_HDFS_PREFIX + DB + ".db/" + TABLE);
    }

    Path path = new Path(parquetMetaInfo.getOriginSourcePath());
    try (DistributedFileSystem dfs = (DistributedFileSystem) DistributedFileSystem
        .newInstance(parquetMetaInfo.getConfiguration())) {

      List<String> allFilePath = getAllFilePath(path, dfs);
      for (String filePath : allFilePath) {
        if (parquetMetaInfoChecker.isAddParquetFile(filePath)) {
          parquetFilePath.add(filePath);
        }
      }
    } catch (IOException e) {
      log.error("not find file path, ", e.getMessage(), e);
    }

    parquetMetaInfo.setPreviousFileList(parquetFilePath);
    parquetMetaInfo.setConcurrent(parquetFilePath.size());
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

  private static void setSchema() {

    try {
      ParquetMetadata parquetMetadata = ParquetFileReader
          .readFooter(ParquetMetaInfoConverter.parquetMetaInfo.getConfiguration(),
              new Path(ParquetMetaInfoConverter.parquetMetaInfo.getPreviousFileList().get(0)),
              ParquetMetadataConverter.NO_FILTER);
      parquetMetaInfo.setSchema(parquetMetadata.getFileMetaData().getSchema());
    } catch (IOException e) {
      throw new RuntimeException(
          "previous file list is empty, maybe incorrect input path or key value column isn't contain in parquet File ");
    }
  }
}