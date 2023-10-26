package org.paradim.empad.com;

import org.apache.commons.cli.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.paradim.empad.dto.DataFileChunk;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;

/*
              #######      #         #       ##########          #            #########
              #            # #     # #       #        #         # #           #        #
              #            #  #   #  #       #        #        #   #          #         #
              #######      #   # #   #       ##########       #######         #          #
              #            #    #    #       #               #       #        #         #
              #            #         #       #              #         #       #        #
              ######       #         #       #             #           #      #########

         version 1.6
         @author: Amir H. Sharifzadeh, The Institute of Data Intensive Engineering and Science, Johns Hopkins University
         @date: 05/25/2023
*/

public class EMPADStreamCommand {

    private static String EMPAD_TOPIC;
    private static String GROUP_ID;
    private static String KAFKA_TEST_CLUSTER_USERNAME;
    private static String KAFKA_TEST_CLUSTER_PASSWORD;


    private static Options initOptions() {

        Options options = new Options();
        options.addOption("i", "info", false, "System Information");
        Option c_op = new Option("c", "config", true, "Config File");

        c_op.setRequired(true);
        options.addOption(c_op);
        return options;
    }

    public static void disableWarning() {
        System.err.close();
        System.setErr(System.out);
    }

    private static int processCommands(Options options, String[] args) {
        int commands = 1;

        if (!Arrays.asList(args).contains("--config")) {
            System.out.println("You should specify the config command with an appropriate config file: --config <config_file_path>");
            return EMPADConstants.ERR_COMMAND;
        }

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("i")) {
                long available = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                System.out.println(available + " MB memory");
            } else if (cmd.hasOption("c")) {
                String configPath = cmd.getOptionValue("config");
                if (configPath != null) {
                    File cf = new File(configPath);
                    if (!cf.exists()) {
                        System.out.println("Config path does not exist or is not accessible!");
                        return EMPADConstants.ERR_COMMAND;
                    }

                    FileInputStream fis = new FileInputStream(cf);

                    Properties properties = new Properties();
                    properties.load(fis);

                    String KAFKA_ENV_USERNAME = properties.getProperty("KAFKA_ENV_USERNAME");
                    if (KAFKA_ENV_USERNAME == null || KAFKA_ENV_USERNAME.length() == 0) {
                        System.out.println("KAFKA_ENV_USERNAME needs to be provided!");
                        return EMPADConstants.ERR_COMMAND;
                    }

                    String KAFKA_ENV_PASSWORD = properties.getProperty("KAFKA_ENV_PASSWORD");
                    if (KAFKA_ENV_PASSWORD == null || KAFKA_ENV_PASSWORD.length() == 0) {
                        System.out.println("KAFKA_ENV_PASSWORD needs to be provided!");
                        return EMPADConstants.ERR_COMMAND;
                    }

                    try {
                        KAFKA_TEST_CLUSTER_USERNAME = System.getenv(KAFKA_ENV_USERNAME);
                    } catch (Exception ex) {
                        System.out.println(KAFKA_ENV_USERNAME + " is wrong or needs to be set in your environment variable!");
                        return EMPADConstants.ERR_COMMAND;
                    }

                    try {
                        KAFKA_TEST_CLUSTER_PASSWORD = System.getenv(KAFKA_ENV_PASSWORD);
                    } catch (Exception ex) {
                        System.out.println(KAFKA_ENV_PASSWORD + " is wrong or needs to be set in your environment variable!");
                        return EMPADConstants.ERR_COMMAND;
                    }

                    if (KAFKA_TEST_CLUSTER_USERNAME == null || KAFKA_TEST_CLUSTER_USERNAME.length() == 0) {
                        System.out.println(KAFKA_ENV_USERNAME + "needs to be set in your environment variable!");
                        return EMPADConstants.ERR_COMMAND;
                    }

                    if (KAFKA_TEST_CLUSTER_PASSWORD == null || KAFKA_TEST_CLUSTER_PASSWORD.length() == 0) {
                        System.out.println(KAFKA_ENV_PASSWORD + "needs to be set in your environment variable!");
                        return EMPADConstants.ERR_COMMAND;
                    }

                    EMPAD_TOPIC = properties.getProperty("EMPAD_TOPIC");
                    if (EMPAD_TOPIC == null || EMPAD_TOPIC.length() == 0) {
                        System.out.println("EMPAD topic needs to be provided!");
                        return EMPADConstants.ERR_COMMAND;
                    }

                    GROUP_ID = properties.getProperty("GROUP_ID");
                    if (GROUP_ID == null || GROUP_ID.length() == 0) {
                        System.out.println("Group ID needs to be provided!");
                        return EMPADConstants.ERR_COMMAND;
                    }
                }
            } else {
                System.out.println("no commands!");
                return EMPADConstants.ERR_COMMAND;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return commands;
    }

    /**
     * <p>This method provides some features to connect Apache Kafka.
     * <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/">Apache Kafka Connector</a>
     * There system environment variable (EMPAD_TOPIC, GROUP_ID, KAFKA_TEST_CLUSTER_USERNAME, and KAFKA_TEST_CLUSTER_PASSWORD) are require to be provided.
     * </p>
     * @throws Exception
     */
    private static void processStream() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        KafkaSource<DataFileChunk> rawSource = KafkaSource.<DataFileChunk>builder().
                setBootstrapServers("pkc-ep9mm.us-east-2.aws.confluent.cloud:9092").
                setTopics(EMPAD_TOPIC).
                setGroupId(GROUP_ID).
                setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule" + " required username=\"" +
                        KAFKA_TEST_CLUSTER_USERNAME + "\" password=\"" +
                        KAFKA_TEST_CLUSTER_PASSWORD + "\";").
                setProperty("security.protocol", "SASL_SSL").
                setProperty("sasl.mechanism", "PLAIN").
                setProperty("enable.auto.commit", "False").
                setStartingOffsets(OffsetsInitializer.earliest()).
                setValueOnlyDeserializer(new DataFileChunkDeserializer()).build();

        DataStream<DataFileChunk> rawDataStream = env.fromSource(rawSource, WatermarkStrategy.noWatermarks(), "EMPAD_TBL");
        processWorkflow(tableEnv, rawDataStream);

        env.execute();
    }

    private static String trimSubDirString(String subDirStr) {
        subDirStr = subDirStr.toLowerCase();
        if (subDirStr.endsWith(EMPADConstants.NOISE_EXT)) {
            return subDirStr.substring(EMPADConstants.NOISE_EXT.length() - 1);
        }
        return subDirStr;
    }

    private static String exclusiveQuery(String column) {
        StringBuilder query_param = new StringBuilder();
        for (int i = 1; i < EMPADConstants.EXCLUSIVE_VARS.length; i++) {
            query_param.append(column).append(" not like '%").append(EMPADConstants.EXCLUSIVE_VARS[i]).append("%' and ");
        }

        query_param.append(column).append(" not like '%").append(EMPADConstants.EXCLUSIVE_VARS[0]).append("%'");

        return query_param.toString();
    }

    /**
     * <p>First we create a temporary view (EMPAD_TBL). Then, we are required to create a normalized query on the stream from a single topic. 
     * I will extract all signals associated with a unique noise. The query will exclude all other data except the XML, noise, and signal files.
     * The results contain
     * 1) the index of the chuck (raw_chunk_i),
     * 2) the total number of chunks (raw_n_total_chunks),
     * 3) the subdirectory where all signals, the noise file, and the XML files are located (raw_subdir_str),
     * 4) the name of each raw file (either signal or noise file),
     * 5) the name of the XML file (opr_filename)
     * </p>
     *
     * @param tableEnv
     * @param rawDataStream
     * @throws Exception
     */
    private static void processWorkflow(StreamTableEnvironment tableEnv, DataStream<DataFileChunk> rawDataStream) throws Exception {

        tableEnv.createTemporaryView("EMPAD_TBL", rawDataStream);

        String signalOperationQuery = "select \n" +
                "       EMPAD_TBL_RAW.chunk_i           as raw_chunk_i,\n" +
                "       EMPAD_TBL_RAW.n_total_chunks    as raw_n_total_chunks,\n" +
                "       EMPAD_TBL_RAW.subdir_str        as raw_subdir_str,\n" +
                "       EMPAD_TBL_RAW.filename          as raw_filename,\n" +
                "       EMPAD_TBL_RAW.data              as raw_data,\n" +
                "       EMPAD_TBL_OPR.filename          as opr_filename,\n" +
                "       EMPAD_TBL_OPR.subdir_str as opr_subdir_str,\n" +
                "       EMPAD_TBL_RAW.experiment as my_experiment\n" +
                "       from EMPAD_TBL EMPAD_TBL_RAW, EMPAD_TBL EMPAD_TBL_OPR\n" +
                "       where (((SUBSTR(EMPAD_TBL_OPR.filename, 1, CHAR_LENGTH (EMPAD_TBL_OPR.filename) - " + EMPADConstants.OPERATION_EXT.length() + ") = EMPAD_TBL_OPR.subdir_str) and\n" +
                "       EMPAD_TBL_RAW.filename = '" + EMPADConstants.RAW_NAME + "' and " +
                "       EMPAD_TBL_OPR.subdir_str = EMPAD_TBL_RAW.subdir_str" +
                "))";


        Table raw_table = tableEnv.sqlQuery(signalOperationQuery);
        tableEnv.toDataStream(raw_table).
                keyBy((KeySelector<Row, String>) row -> String.valueOf(row.getField(EMPADConstants.PROTECTED_KEY_ID))).
                process(new StreamingSignalProcessing()).setParallelism(8);
    }

    /**
     * <p> Main method (The program starts here).
     * These system environment variable (KAFKA_TEST_CLUSTER_USERNAME, KAFKA_TEST_CLUSTER_PASSWORD, GROUP_ID, and EMPAD_TOPIC) are being provided.
     * </p>
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        disableWarning();

        KAFKA_TEST_CLUSTER_USERNAME = System.getenv("KAFKA_ENV_USERNAME");
        KAFKA_TEST_CLUSTER_PASSWORD = System.getenv("KAFKA_ENV_PASSWORD");

        GROUP_ID = System.getenv("GROUP_ID");
        EMPAD_TOPIC = System.getenv("EMPAD_TOPIC");

        System.out.println("EMPAD_TOPIC = " + EMPAD_TOPIC);
        System.out.println("GROUP_ID = " + GROUP_ID);

        processStream();
    }
}
