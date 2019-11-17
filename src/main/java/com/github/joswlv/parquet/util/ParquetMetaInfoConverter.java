package com.github.joswlv.parquet.util;

import com.github.joswlv.parquet.metadata.ParquetMetaInfo;
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
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class ParquetMetaInfoConverter {

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
        .desc("Set multi-thread pool size")
        .hasArg()
        .required(true)
        .build());

    options.addOption(Option.builder().longOpt("source-path")
        .desc("target parquet file dir path")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("compression")
        .desc(
            "compression codec name of new parquete file, when not input impala default value")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("block-size")
        .desc("blocksize of new parquete file, when not input impal adefault value")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("page-size")
        .desc("pagesize of new parquete file, when not input impala default value")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("dict-page-size")
        .desc("dictionarypageSize of new parquete file, when not input impala default value")
        .hasArg()
        .build());

    options.addOption(Option.builder().longOpt("writer-version")
        .desc(
            "new parquete file writer version, when not input impala default value [ example : PARQUET_1_0, PARQUET_2_0]")
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
      e.printStackTrace();
    }
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("help", options);

    parquetMetaInfo = ParquetMetaInfo.builder()
        .tableFullName(cl.getOptionValue("table-name"))
        .keyColName(cl.getOptionValue("key-col-name"))
        .keyColValueList(Arrays.asList(cl.getOptionValue("key-col-value")))
        .targetColNameList(Arrays.asList(cl.getOptionValue("target-col-name")))
        .concurrent(Integer.parseInt(cl.getOptionValue("concurrent")))
        .configuration(new Configuration())
        .build();

    if (cl.hasOption("target-path")) {
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
    setSchema();
    return parquetMetaInfo;
  }

  private static void setPreviousFileList() {
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
    try {
      fs = path.getFileSystem(parquetMetaInfo.getConfiguration());
      FileStatus[] fileStatuses = fs.listStatus(path);

      for (FileStatus fileStatus : fileStatuses) {
        if (fileStatus.isFile() && fileStatus.getBlockSize() > 0) {
          parquetFilePath.add(fileStatus.getPath().toString());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    parquetMetaInfo.setPreviousFileList(parquetFilePath);
  }

  private static void setSchema() {
    try {
      ParquetMetadata parquetMetadata = ParquetFileReader
          .readFooter(ParquetMetaInfoConverter.parquetMetaInfo.getConfiguration(),
              new Path(ParquetMetaInfoConverter.parquetMetaInfo.getPreviousFileList().get(0)),
              ParquetMetadataConverter.NO_FILTER);
      parquetMetaInfo.setSchema(parquetMetadata.getFileMetaData().getSchema());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
