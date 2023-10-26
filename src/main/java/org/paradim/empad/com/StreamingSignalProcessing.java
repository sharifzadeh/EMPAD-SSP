package org.paradim.empad.com;

import com.dynatrace.dynahist.Histogram;
import com.dynatrace.dynahist.layout.CustomLayout;
import com.dynatrace.dynahist.layout.Layout;
import com.jmatio.io.MatFileReader;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.msgpack.value.BinaryValue;
import org.paradim.empad.dto.MaskTO;
import org.testcontainers.shaded.org.apache.commons.lang3.SerializationUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.jmatio.types.*;

import static org.paradim.empad.com.EMPADConstants.EMPAD_HOME;

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
         @date: 06/14/2023
         @last modified: 10/15/2023
*/

public class StreamingSignalProcessing extends ProcessFunction<Row, String> {
    private transient ValueState<MaskTO> maskState;
    private transient ValueState<String> noiseValue;
    //    private transient MapState<String, Instant> timeInstantMapState;
//    private transient MapState<String, Duration> timeDurationMapState;
    private transient ValueState<String> osSlashValue;
    private transient MapState<String, Integer> countMap;
    private MapState<String, Integer> totalMap;

    private transient MapState<String, Integer> rawDimension;

    private transient MapState<String, Long> processedRaw;


    @Override
    public void processElement(Row row, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {

        if (osSlashValue.value() == null || osSlashValue.value().length() == 0) {
            if (System.getProperty("os.name").toLowerCase().contains("windows")) {
                osSlashValue.update("\\");
            } else {
                osSlashValue.update("/");
            }
        }

        String stateDir = String.valueOf(row.getField(EMPADConstants.SUBDIR_STR));
        String slash = osSlashValue.value();
        String tempPath = EMPAD_HOME + slash + "temp" + slash;
        String statePath = tempPath + stateDir;

        if (processedRaw.get(stateDir) == null && stateDir.contains(EMPADConstants.NOISE_EXT) &&
                Files.exists(Paths.get(EMPAD_HOME + slash + "means" + slash + stateDir))) {

            processedRaw.put(stateDir, (long) 0);

            noiseValue.update(stateDir);

            System.out.println("===========================================================================================");
            System.out.println("Processed Mean Detected: " + EMPAD_HOME + slash + "means" + slash + stateDir);
            System.out.println("===========================================================================================");
        }


        if (processedRaw.get(stateDir) == null) {

            if (maskState.value() == null) {
                String calibrationPath = EMPAD_HOME + slash + "mask" + slash + "mask.mat";
                MatFileReader matfilereader = new MatFileReader(calibrationPath);

                float[][] g1A = toFloat(((MLDouble) matfilereader.getMLArray("g1A")).getArray());
                float[][] g1B = toFloat(((MLDouble) matfilereader.getMLArray("g1B")).getArray());
                float[][] g2A = toFloat(((MLDouble) matfilereader.getMLArray("g2A")).getArray());
                float[][] g2B = toFloat(((MLDouble) matfilereader.getMLArray("g2B")).getArray());
                float[][] offA = toFloat(((MLDouble) matfilereader.getMLArray("offA")).getArray());
                float[][] offB = toFloat(((MLDouble) matfilereader.getMLArray("offB")).getArray());

                float[][] flatfA = toFloat(((MLDouble) matfilereader.getMLArray("flatfA")).getArray());
                float[][] flatfB = toFloat(((MLDouble) matfilereader.getMLArray("flatfB")).getArray());

                MaskTO maskTO = new MaskTO(g1A, g1B, g2A, g2B, offA, offB, flatfA, flatfB);
                maskState.update(maskTO);
            }

            if (!Files.exists(Paths.get(statePath))) {
                Files.createDirectories(Paths.get(statePath));
            }

            int chunkId = Integer.parseInt(String.valueOf(row.getField(EMPADConstants.CHUNK_ID)));

            int rawTotalChunk = Integer.parseInt(String.valueOf(row.getField(EMPADConstants.TOTAL_CHUNK)));

            BinaryValue raw_data_chunk = ((BinaryValue) (row.getField(EMPADConstants.DATA)));
            assert raw_data_chunk != null;

            int chunkSize = raw_data_chunk.asByteArray().length;

            String rawType = "Signal";
            if (totalMap.get(stateDir) == null) {
                if (stateDir.contains(EMPADConstants.NOISE_EXT)) {
                    rawType = "Noise";
                }

                System.out.println("===========================================================================================");
                System.out.println(rawType + " Detected: " + stateDir + " | precision: " + EMPADConstants.precision + " | Total chunk = " + rawTotalChunk +
                        " | Size = " + (rawTotalChunk / 100.00) * (chunkSize / 10000000.00) + " GB");
                System.out.println("===========================================================================================");
            }

            totalMap.put(stateDir, rawTotalChunk);

            int countState;
            if (countMap.get(stateDir) == null) {
                countMap.put(stateDir, 1);
            } else {
                countState = countMap.get(stateDir);
                countMap.put(stateDir, countState + 1);
            }

            if (noiseValue.value().length() == 0) {
                if (stateDir.contains(EMPADConstants.NOISE_EXT)) {
                    noiseValue.update(stateDir);
                }
            }

//            if (timeInstantMapState.get(stateDir) == null) {
//                Instant now = Instant.now();
//                timeInstantMapState.put(stateDir, now);
//
//                timeDurationMapState.put(stateDir, Duration.between(now, now));
//            }

//            if (timeInstantMapState.get(stateDir) != null) {
//                Instant now = Instant.now();
//                Instant prev = timeInstantMapState.get(stateDir);
//
//                timeDurationMapState.put(stateDir, Duration.between(now, prev).plus(timeDurationMapState.get(stateDir)));
//            }

//            if (timeInstantMapState.get(stateDir) != null) {
//                Instant now = Instant.now();
//                Instant prev = timeInstantMapState.get(stateDir);
//                timeDurationMapState.put(stateDir, Duration.between(prev, now));
//            }

            double[][][] rawFrames = process(chunkId, chunkSize, raw_data_chunk, maskState.value());

//            if (timeInstantMapState.get(stateDir) != null) {
//                Instant now = Instant.now();
//                Instant prev = timeInstantMapState.get(stateDir);
//                timeDurationMapState.put(stateDir, Duration.between(prev, now));
//            }

            SerializationUtils.serialize(rawFrames, new FileOutputStream(statePath + slash + chunkId));

            if (countMap.get(stateDir) % 100 == 0) {
                System.out.println(stateDir + " : " + countMap.get(stateDir) + " of " + totalMap.get(stateDir) + " processed.");
            }

            if (rawDimension.get(stateDir) == null) {
                rawDimension.put(stateDir, rawFrames.length);
            }

            int count, finalRawFrameLen, totalFrames, s;

            Tuple2<double[][], double[][]> means;
            double[][][] imageObjArray;
            double[][][] finalRawFrame;
            File[] listOfFiles;

            String noise = noiseValue.value();

            String meansPath = EMPAD_HOME + slash + "means" + slash + noise;

            if (!Files.exists(Paths.get(meansPath)) && processedRaw.get(noise) == null) {

                listOfFiles = new File(tempPath + noise).listFiles();

                count = countMap.get(noise);

                if (noiseValue.value().length() != 0 && totalMap.get(noise) == count) {
                    assert listOfFiles != null;
                    if (listOfFiles.length >= countMap.get(noise)) {

                        finalRawFrame = SerializationUtils.deserialize(new FileInputStream(tempPath + noise + slash + count));
                        finalRawFrameLen = finalRawFrame.length;

                        totalFrames = (count - 1) * rawDimension.get(noise) + finalRawFrameLen;

                        System.out.println(noise + ": Total Frames = " + totalFrames);

                        imageObjArray = new double[totalFrames][128][128];

                        s = rawDimension.get(noise);

//                        if (timeInstantMapState.get(noise) != null) {
//                            Instant now = Instant.now();
//                            Instant prev = timeInstantMapState.get(stateDir);
//                            timeDurationMapState.put(noise, Duration.between(prev, now).plus(timeDurationMapState.get(noise)));
//                            System.out.println(timeDurationMapState.get(noise).getSeconds());
//                        }

                        for (int chId = 0; chId < count - 1; chId++) {
                            rawFrames = SerializationUtils.deserialize(new FileInputStream(tempPath + noise + slash + (chId + 1)));
                            System.arraycopy(rawFrames, 0, imageObjArray, s * chId, s);
                        }

//                        if (timeInstantMapState.get(noise) != null) {
//                            Instant now = Instant.now();
//                            Instant prev = timeInstantMapState.get(stateDir);
//                            timeDurationMapState.put(noise, Duration.between(prev, now).plus(timeDurationMapState.get(noise)));
//                            System.out.println(timeDurationMapState.get(noise).getSeconds());
//                        }

                        for (int i = 0; i < finalRawFrameLen; i++) {
                            imageObjArray[totalFrames - (finalRawFrameLen - i)] = finalRawFrame[i];
                        }
//                        System.arraycopy(finalRawFrame, 0, imageObjArray, (count - 1) * finalRawFrameLen, finalRawFrameLen);

                        means = noiseMeans(totalFrames, imageObjArray);
                        SerializationUtils.serialize(means, new FileOutputStream(meansPath));

                        System.out.println("Processed Noise Mean Value.");

                        processedRaw.put(stateDir, (long) 0);

                        try {
                            FileUtils.deleteDirectory(new File(tempPath + noise));
                        } catch (Exception ignored) {

                        }

                    }
                }
            }

            Tuple2<double[][], double[][]> meansObj;

            if (processedRaw.get(noise) != null) {
                Iterable<String> signalKeys = totalMap.keys();

                for (String signal : signalKeys) {
                    if (!Files.exists(Paths.get(meansPath)) || processedRaw.get(signal) != null) {
                        continue;
                    }

                    count = countMap.get(signal);

                    if (!signal.equals(noise) && totalMap.get(signal) == count) {

                        meansObj = SerializationUtils.deserialize(new FileInputStream(meansPath));

                        finalRawFrame = SerializationUtils.deserialize(new FileInputStream(tempPath + signal + slash + count));
                        finalRawFrameLen = finalRawFrame.length;

                        totalFrames = (count - 1) * rawDimension.get(signal) + finalRawFrameLen;

                        System.out.println(signal + ": Total Frames = " + totalFrames);

                        imageObjArray = new double[totalFrames][128][128];

                        s = rawDimension.get(signal);

//                        if (timeInstantMapState.get(signal) != null) {
//                            Instant now = Instant.now();
//                            Instant prev = timeInstantMapState.get(signal);
//                            timeDurationMapState.put(signal, Duration.between(prev, now).plus(timeDurationMapState.get(signal)));
//                        }

                        for (int chId = 0; chId < count - 1; chId++) {
                            rawFrames = SerializationUtils.deserialize(new FileInputStream(statePath + slash + (chId + 1)));
                            System.arraycopy(rawFrames, 0, imageObjArray, s * chId, s);
                        }

                        for (int i = 0; i < finalRawFrameLen; i++) {
                            imageObjArray[totalFrames - (finalRawFrameLen - i)] = finalRawFrame[i];
                        }
//                        if (timeInstantMapState.get(signal) != null) {
//                            Instant now = Instant.now();
//                            Instant prev = timeInstantMapState.get(signal);
//                            timeDurationMapState.put(stateDir, Duration.between(prev, now));
//                        }

//                        System.arraycopy(finalRawFrame, 0, imageObjArray, (count - 1) * finalRawFrameLen, finalRawFrameLen);

//                        if (!Files.exists(Paths.get(tempPath + "prc"))) {
//                            Files.createDirectories(Paths.get(tempPath + "prc"));
//                        }

//                        SerializationUtils.serialize(imageObjArray, new FileOutputStream(tempPath + "prc" + slash + signal + "_prc.raw"));

                        try {
                            FileUtils.deleteDirectory(new File(tempPath + signal));
                        } catch (Exception ignored) {

                        }


                        System.out.println(signal + " just processed!");

//                        if (timeInstantMapState.get(signal) != null) {
//                            Instant now = Instant.now();
//                            Instant prev = timeInstantMapState.get(signal);
//                            timeDurationMapState.put(stateDir, Duration.between(prev, now));
//                        }

                        combine_from_concat_EMPAD2(signal, maskState.value(), slash, totalFrames, imageObjArray, meansObj);

                        processedRaw.put(signal, 0L);

//                        FileUtils.delete(new File(tempPath + "prc" + slash + signal + "_prc.raw"));

//                        if (timeInstantMapState.get(signal) != null) {
//                            Instant now = Instant.now();
//                            Instant prev = timeInstantMapState.get(signal);
//                            timeDurationMapState.put(stateDir, Duration.between(prev, now));
//                        }

//                        System.out.println("The duration of processing " + signal + " was: " + timeDurationMapState.get(stateDir) + " seconds.");
//                        System.gc();
                    }
                }
            }
        }
    }

    @Override
    public void open(Configuration parameters) {

//        MapStateDescriptor<String, Duration> timeDurationStateMapDescriptor =
//                new MapStateDescriptor<>(
//                        "timeDurationMapState",
//                        Types.STRING,
//                        TypeInformation.of(Duration.class));
//        timeDurationMapState = getRuntimeContext().getMapState(timeDurationStateMapDescriptor);
//
//        MapStateDescriptor<String, Instant> timeInstantStateMapDescriptor =
//                new MapStateDescriptor<>(
//                        "timeInstantMapState",
//                        Types.STRING,
//                        TypeInformation.of(Instant.class));
//        timeInstantMapState = getRuntimeContext().getMapState(timeInstantStateMapDescriptor);

        ValueStateDescriptor<MaskTO> maskStateDescriptor =
                new ValueStateDescriptor<>(
                        "maskState",
                        TypeInformation.of(MaskTO.class));
        maskState = getRuntimeContext().getState(maskStateDescriptor);

        MapStateDescriptor<String, Integer> totalMapStateDescriptor =
                new MapStateDescriptor<>(
                        "totalMapState",
                        Types.STRING,
                        Types.INT);
        totalMap = getRuntimeContext().getMapState(totalMapStateDescriptor);

        ValueStateDescriptor<String> noiseStateDescriptor =
                new ValueStateDescriptor<>(
                        "noiseState",
                        Types.STRING,
                        "");
        noiseValue = getRuntimeContext().getState(noiseStateDescriptor);

        ValueStateDescriptor<String> osSlashStateDescriptor =
                new ValueStateDescriptor<>(
                        "osSlashState",
                        Types.STRING,
                        "");
        osSlashValue = getRuntimeContext().getState(osSlashStateDescriptor);

        MapStateDescriptor<String, Integer> rawDimensionMapStateDescriptor =
                new MapStateDescriptor<>(
                        "rawDimensionMapState",
                        Types.STRING,
                        Types.INT);
        rawDimension = getRuntimeContext().getMapState(rawDimensionMapStateDescriptor);

        MapStateDescriptor<String, Integer> countMapStateDescriptor =
                new MapStateDescriptor<>(
                        "countMapState",
                        Types.STRING,
                        TypeInformation.of(new TypeHint<>() {
                        }));
        countMap = getRuntimeContext().getMapState(countMapStateDescriptor);

        MapStateDescriptor<String, Long> processedRawDescriptor =
                new MapStateDescriptor<>(
                        "processedRawSate",
                        Types.STRING,
                        Types.LONG);
        processedRaw = getRuntimeContext().getMapState(processedRawDescriptor);

    }

    /**
     * <p>This method is equivalent of NumPy unpack function's implementation for both gloat and unsigned integer</p>
     *
     * @param type
     * @param dim
     * @param raw
     * @return object
     */

    public Object unpack(char type, int dim, byte[] raw) {
        if (type == 'f') {
            var floats = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
            var floatArray = new float[dim];
            floats.get(floatArray);
            return floatArray;
        } else if (type == 'I') {
            var ints = ByteBuffer.wrap(raw).order(ByteOrder.nativeOrder()).asIntBuffer();
            var intArray = new int[dim];
            ints.get(intArray);

            return Arrays.stream(intArray).mapToLong(Integer::toUnsignedLong).toArray();
        }
        return null;
    }

    /**
     * <p>This method converts 2D double to 2D float</p>
     *
     * @param data
     * @return 2D float
     */
    public float[][] toFloat(double[][] data) {
        float[][] flMat = new float[data.length][data[0].length];
        for (int i = 0; i < flMat.length; i++) {
            for (int j = 0; j < flMat[0].length; j++) {
                flMat[i][j] = (float) data[i][j];
            }
        }
        return flMat;
    }

    public float[][] reshape1_to_2(float[] array, int rows, int cols) {
        if (array.length != (rows * cols)) throw new IllegalArgumentException("Invalid array length");

        float[][] array2d = new float[rows][cols];
        for (int i = 0; i < rows; i++)
            System.arraycopy(array, (i * cols), array2d[i], 0, cols);

        return array2d;
    }

    /**
     * <p>This method converts a double array to a 2D double array. The dimensions should be justified properly</p>
     *
     * @param array
     * @param rows
     * @param cols
     * @return
     */
    public double[][] reshape1_to_2(double[] array, int rows, int cols) {
        if (array.length != (rows * cols)) throw new IllegalArgumentException("Invalid array length");

        double[][] array2d = new double[rows][cols];
        for (int i = 0; i < rows; i++)
            System.arraycopy(array, (i * cols), array2d[i], 0, cols);

        return array2d;
    }

    /**
     * <p>This method converts a double array to a 3D double array. The dimensions should be justified properly</p>
     *
     * @param data
     * @param width
     * @param height
     * @param depth
     * @return
     */
    public double[][][] reshape1_to_3_float(double[] data, int width, int height, int depth) {
        if (data.length != (width * height * depth)) throw new IllegalArgumentException("Invalid array length");

        double[][][] array3d = new double[width][height][depth];

        for (int x = 0; x < width; x++) {
            for (int y = 0; y < height; y++) {
                for (int z = 0; z < depth; z++) {
                    array3d[x][y][z] = data[height * depth * x + depth * y + z];
                }
            }
        }
        return array3d;
    }

    /**
     * <p>This method is equivalent of NumPy * implementation which is Hadamard product of two 2D double arrays hadamard</p>
     *
     * @param m1
     * @param m2
     * @return 2D double array
     */
    public double[][] hadamard(double[][] m1, double[][] m2) {
        double[][] res = new double[m1.length][m1[0].length];
        for (int i = 0; i < m1.length; i++) {
            for (int j = 0; j < m1[0].length; j++) {
                res[i][j] = m1[i][j] * m2[i][j];
            }
        }
        return res;
    }

    /**
     * <p>This method is equivalent of NumPy * implementation which is Hadamard product of two 2D float arrays hadamard</p>
     *
     * @param m1
     * @param m2
     * @return 2D double array
     */
    public double[][] hadamard(float[][] m1, double[][] m2) {
        double[][] res = new double[m1.length][m1[0].length];
        for (int i = 0; i < m1.length; i++) {
            for (int j = 0; j < m1[0].length; j++) {
                res[i][j] = m1[i][j] * m2[i][j];
            }
        }
        return res;
    }

    /**
     * <p>This method adds two double arrays</p>
     *
     * @param m1
     * @param m2
     * @return 2D array
     */
    public double[][] add2mat(double[][] m1, double[][] m2) {
        double[][] res = new double[m1.length][m1[0].length];
        for (int i = 0; i < m1.length; i++) {
            for (int j = 0; j < m1[0].length; j++) {
                res[i][j] = m1[i][j] + m2[i][j];
            }
        }
        return res;
    }

    /**
     * <p>This method adds a 2D double array to a 2D float array</p>
     *
     * @param m1
     * @param m2
     * @return 2D float array
     */
    public float[][] add2mat(float[][] m1, double[][] m2) {
        float[][] res = new float[m1.length][m1[0].length];
        for (int i = 0; i < m1.length; i++) {
            for (int j = 0; j < m1[0].length; j++) {
                res[i][j] = (float) (m1[i][j] + m2[i][j]);
            }
        }
        return res;
    }

    /**
     * <p>This method subtracts a 2D double array from another 2D double array </p>
     *
     * @param m1
     * @param m2
     * @return 2D double array
     */
    public double[][] minus2mat(double[][] m1, double[][] m2) {
        double[][] res = new double[m1.length][m1[0].length];
        for (int i = 0; i < m1.length; i++) {
            for (int j = 0; j < m1[0].length; j++) {
                res[i][j] = m1[i][j] - m2[i][j];
            }
        }
        return res;
    }

    /**
     * <p>This method subtracts a 2D double array from a 2D float array</p>
     *
     * @param m1
     * @param m2
     * @return 2D double array
     */
    public double[][] minus2mat(double[][] m1, float[][] m2) {
        double[][] res = new double[m1.length][m1[0].length];
        for (int i = 0; i < m1.length; i++) {
            for (int j = 0; j < m1[0].length; j++) {
                res[i][j] = m1[i][j] - m2[i][j];
            }
        }
        return res;
    }

    /**
     * This method applies some filters into the results of an array of unsigned unpacked. The scientific specification is based on the MATLAB code from Cornel University
     *
     * @param chId
     * @param nVals
     * @param g1A
     * @param g1B
     * @param g2A
     * @param g2B
     * @param offA
     * @param offB
     * @return 3D double array
     * @throws IOException
     */
    public double[][][] PAD_AB_bin2data(int chId, long[] nVals, float[][] g1A, float[][] g1B, float[][] g2A, float[][] g2B, float[][] offA, float[][] offB) throws IOException {

        int nLen = nVals.length;
        int nFrames = nLen / 128 / 128;

        double[] ana = new double[nLen];
        for (int i = 0; i < nLen; i++)
            ana[i] = nVals[i] & 16383;

        double[][][] ana3d = reshape1_to_3_float(ana, nFrames, 128, 128);

        double[] dig = new double[nLen];
        for (int i = 0; i < nLen; i++)
            dig[i] = (double) (nVals[i] & 1073725440) / 16384;

        double[][][] dig3d = reshape1_to_3_float(dig, nFrames, 128, 128);

        long gnl;
        double[] gn = new double[nLen];
        String td = "2147483648";
        for (int i = 0; i < nLen; i++) {
            gnl = (nVals[i]) & Long.parseLong(td);
            gn[i] = (double) gnl / 65536 / 16384 / 2;
        }

        double[][][] gn3d = reshape1_to_3_float(gn, nFrames, 128, 128);

        double[][] ones_2 = new double[128][128];
        for (double[] row : ones_2) {
            Arrays.fill(row, 1.0);
        }

        double[][] term1_1, term1, term2_1, term2_2, term2, term3, term5;

        for (int i = 0; i < nFrames; i += 2) {
            term1_1 = minus2mat(ones_2, gn3d[i]);
            term1 = hadamard(ana3d[i], term1_1);
            term2_1 = minus2mat(ana3d[i], offA);
            term2_2 = hadamard(g1A, term2_1);
            term2 = hadamard(term2_2, gn3d[i]);
            term3 = hadamard(g2A, dig3d[i]);
            term5 = add2mat(term1, term2);
            ana3d[i] = add2mat(term5, term3);

            term1_1 = minus2mat(ones_2, gn3d[i + 1]);
            term1 = hadamard(ana3d[i + 1], term1_1);
            term2_1 = minus2mat(ana3d[i + 1], offB);
            term2_2 = hadamard(g1B, term2_1);
            term2 = hadamard(term2_2, gn3d[i + 1]);
            term3 = hadamard(g2B, dig3d[i + 1]);
            term5 = add2mat(term1, term2);
            ana3d[i + 1] = add2mat(term5, term3);
        }

        return ana3d;
    }

    /**
     * This method unpacked the packet (unsigned integer) and applies the results into six filters
     *
     * @param chId
     * @param chunkSize
     * @param dataBinaryChunk
     * @param maskTO
     * @return 3D double array
     * @throws IOException
     */
    public double[][][] combineConcatenatedEMPAD2ABLarge(int chId, int chunkSize, BinaryValue dataBinaryChunk, MaskTO maskTO) throws IOException {
        float[][] g1A, g1B, g2A, g2B, offA, offB;

        g1A = maskTO.getG1A();
        g1B = maskTO.getG1B();
        g2A = maskTO.getG2A();
        g2B = maskTO.getG2B();
        offA = maskTO.getOffA();
        offB = maskTO.getOffB();

        byte[] chunkByte;
        long[] nVals_i;
        chunkByte = dataBinaryChunk.asByteArray();
        nVals_i = (long[]) unpack('I', chunkSize / 4, chunkByte);

        assert nVals_i != null;
        return PAD_AB_bin2data(chId, nVals_i, g1A, g1B, g2A, g2B, offA, offB);
    }

    /**
     * <p>This is initiation of signal processing. A deserialized packet and a transforming abject which carries filters will be performed</p>
     *
     * @param chId
     * @param chunkSize
     * @param dataBinaryChunk
     * @param maskTO
     * @return 3D double array
     * @throws IOException
     */
    public double[][][] process(int chId, int chunkSize, BinaryValue dataBinaryChunk, MaskTO maskTO) throws IOException {
        return combineConcatenatedEMPAD2ABLarge(chId, chunkSize, dataBinaryChunk, maskTO);
    }

    /**
     * <p>This method is implementation of NumPy mean's function where axis = 0.</p>
     *
     * @param bkgdObjArray
     * @param s
     * @return 2D double array
     */
    public double[][] calculateMean(double[][][] bkgdObjArray, int s) {
        int l = bkgdObjArray.length / 2;
        double[][][] bkgdDataArray = new double[l][128][128];

        for (int i = 0; i < l; i++) {
            bkgdDataArray[i] = bkgdObjArray[i * 2 + s];
        }

        double[][] meanBkgd = new double[128][128];

        for (int i = 0; i < 128; i++) {
            for (int j = 0; j < 128; j++) {
                for (int k = 0; k < l; k++) {
                    meanBkgd[i][j] += bkgdDataArray[k][i][j];
                }
                meanBkgd[i][j] /= l;
            }
        }

        return meanBkgd;
    }

    /**
     * This method generates an array of double values in the bracket of [start..end] with a 10 step
     *
     * @param start
     * @param end
     * @return array of doubles
     */
    public double[] arange(double start, double end) {
        return IntStream.rangeClosed(0, (int) ((end - start) / 10)).mapToDouble(x -> x * 10 + start).toArray();
    }

    /**
     * This method find the index of the largest value in an array
     * @param arr
     * @return int
     */
    public int largestIndex(double[] arr) {
        int l = -1;
        if (arr == null || arr.length == 0)
            return l;

        l = 0;
        for (int i = 1; i < arr.length; i++)
            if (arr[i] > arr[l]) l = i;
        return l;
    }

    /**
     * <a>This method generates a layout for histogram</a>
     * @param start
     * @param end
     * @return Layout object
     */
    public Layout createLayout(double start, double end) {
        return CustomLayout.create(IntStream.rangeClosed(0, (int) ((end - start) / 10)).mapToDouble(x -> x * 10 + start).toArray());
    }

    /**
     * <a>This method flats a 2D float array into an array</a>
     * @param matrix
     * @return array of floats
     */
    public float[] flattenedFloat(float[][] matrix) {
        float[] flattenedArray = new float[matrix.length * matrix[0].length];
        int count = 0;
        for (float[] floats : matrix) {
            for (int k = 0; k < matrix[0].length; k++) {
                flattenedArray[count++] = floats[k];
            }
        }
        return flattenedArray;
    }

    /**
     * a>This method flats a 3D float array into an array</a>
     * @param matrix
     * @return array of floats
     */
    public double[] flattenedFloat(float[][][] matrix) {
        double[] flattenedArray = new double[matrix.length * matrix[0].length * matrix[0][0].length];
        int count = 0;
        for (float[][] floats : matrix) {
            for (int j = 0; j < matrix[0].length; j++) {
                for (int k = 0; k < matrix[0][0].length; k++) {
                    flattenedArray[count++] = floats[j][k];
                }
            }
        }
        return flattenedArray;
    }

    /**
     * <a>This method applies the histogram and several statistics calculations. The scientific specification is based on the MATLAB code from Cornel University.
     * @param npMat
     * @return Tuple3
     */
    public Tuple3<double[][], Integer, Integer> debounce_f(double[][] npMat) {
        double range1 = (-200.00 - ((float) 10 / 2));
        double range2 = (220.00 - ((float) 10 / 2));
        double[] edges = arange(range1, range2);
        double[] npMatFlat = Stream.of(npMat).flatMapToDouble(DoubleStream::of).toArray();

        Layout layout = CustomLayout.create(edges);
        Histogram histogram = Histogram.createStatic(layout);
        for (double v : npMatFlat) {
            histogram.addValue(v);
        }

        double[] histVal = IntStream.range(histogram.getLayout().getUnderflowBinIndex() + 1,
                histogram.getLayout().getOverflowBinIndex()).mapToDouble(histogram::getCount).toArray();

        int histMaxArg = largestIndex(histVal);

        double histMaxVal = histVal[histMaxArg];

        int nNumPoint = 2 * 3 + 1;

        double offset;

        double[] offsetArr = new double[npMatFlat.length];
        double[] npNewMat = new double[npMatFlat.length];

        if (histMaxVal > 40) {

            int[] wVal = new int[2 * 3 + 1];
            for (int i = 0; i < wVal.length; i++)
                wVal[i] = -3 + i;

            int nInd1 = Math.max(histMaxArg - 3, 0);
            int nInd2 = Math.min(histMaxArg + 3 + 1, histVal.length);
            double[] currentHist = new double[Math.abs(nInd1 - nInd2)];
            System.arraycopy(histVal, nInd1, currentHist, 0, currentHist.length);

            double sum_y = DoubleStream.of(currentHist).sum();
            double sum_xy = 0;
            double sum_x2y = 0;
            double sum_x2 = 0;
            double sum_x4 = 0;

            int min = Math.min(wVal.length, currentHist.length);

            for (int i = 0; i < min; i++) {
                sum_xy += wVal[i] * currentHist[i];
                sum_x2y += Math.pow(wVal[i], 2) * currentHist[i];
                sum_x2 += Math.pow(wVal[i], 2);
                sum_x4 += Math.pow(wVal[i], 4);
            }

            double bVal = sum_xy / sum_x2;
            double aVal = (nNumPoint * sum_x2y - sum_x2 * sum_y) / (nNumPoint * sum_x4 - sum_x2 * sum_x2);

            double comx = 0;
            if (Math.abs(aVal) > 0.0001) {
                comx = -bVal / (2 * aVal);
            }

            offset = ((float) edges[histMaxArg] + ((float) 10 / 2.0) + (comx * 10));
            if (Math.abs(offset) > 200) {
                offset = 0;
            }
        } else {
            offset = 0;
        }

        Arrays.fill(offsetArr, offset);

        for (int i = 0; i < npMatFlat.length; i++) {
            npNewMat[i] = npMatFlat[i] - offsetArr[i];
        }

        return new Tuple3<>(reshape1_to_2(npNewMat, 128, 128), (int) histMaxVal, histMaxArg);
    }

    /**
     * This method calculates the mean value of a 3D double array.
     * @param nFramesBack
     * @param noiseObjArray
     * @return Tuple2
     */
    public Tuple2<double[][], double[][]> noiseMeans(int nFramesBack, double[][][] noiseObjArray) {
        double[][] bkgedata, bkgodata;

        bkgodata = calculateMean(noiseObjArray, 0);

        if (nFramesBack > 1) {
            bkgedata = calculateMean(noiseObjArray, 1);
        } else {
            bkgedata = new double[128][128];
        }

        return new Tuple2<>(bkgodata, bkgedata);
    }

    /**
     * <p>This method finalizes the results from a signal and mean values. The scientific specification is based on the MATLAB code from Cornel University.
     * The final results will be corrected and will be saved as a raw object into a local file system.
     * </p>
     * @param signal
     * @param maskTO
     * @param slash
     * @param totalFrames
     * @param imageObjArray
     * @param means
     * @throws Exception
     */
    public void combine_from_concat_EMPAD2(String signal, MaskTO maskTO, String slash, int totalFrames, double[][][] imageObjArray, Tuple2<double[][], double[][]> means) throws Exception {

        float[][] flatfA = maskTO.getFlatfA();
        float[][] flatfB = maskTO.getFlatfB();

        double[][] bkgodata = means.f0;
        double[][] bkgedata = means.f1;

        for (int i = 0; i < totalFrames; i += 2) {
            imageObjArray[i] = minus2mat(imageObjArray[i], bkgodata);
            imageObjArray[i + 1] = minus2mat(imageObjArray[i + 1], bkgedata);
        }

        bkgodata = null;
        bkgedata = null;

        System.out.println("Debouncing: " + signal);
        FileWriter writer = new FileWriter(EMPAD_HOME + slash + "histogram" + slash + "hist_" + signal + ".txt", true);
        Tuple3<double[][], Integer, Integer> debounceHistValue;
        for (int i = 0; i < totalFrames; i++) {
            debounceHistValue = debounce_f(imageObjArray[i]);
            imageObjArray[i] = debounceHistValue.f0;
            // In case of Matlab indices starts from 1, debounceHistValue.f2++
            writer.write(debounceHistValue.f1 + " : " + (debounceHistValue.f2 + 1));
            writer.write(System.getProperty("line.separator"));
        }
        writer.flush();
        writer.close();

        System.out.println("Transforming Filters: " + signal);
        double[][] data1;
        double[][] data2;
        int a, b;
        for (int i = 0; i < totalFrames / 2; i++) {
            a = 2 * i;
            b = 2 * i + 1;
            data1 = imageObjArray[a];
            data2 = imageObjArray[b];
            for (int j = 0; j < 128; j++) {
                for (int k = 0; k < 128; k++) {
                    imageObjArray[a][j][k] = data1[j][k] * flatfA[j][k];
                    imageObjArray[b][j][k] = data2[j][k] * flatfB[j][k];
                }
            }
        }

        String tempSignalName = "temp_out_" + signal + ".raw";
        String tempSignalOutFilePath = EMPAD_HOME + slash + "temp" + slash + "out" + slash;

        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream(tempSignalOutFilePath + tempSignalName)))) {
            for (int i = 0; i < totalFrames; i++) {
                for (int j = 0; j < 128; j++) {
                    for (int k = 0; k < 128; k++) {
                        out.writeFloat((float) imageObjArray[i][j][k]);
                    }
                }
            }
            out.flush();
        }

        imageObjArray = null;
        means = null;
        maskTO = null;

        String outFilePath = EMPAD_HOME + slash + "output" + slash;

//        String outFilePath = EMPAD_HOME + slash + "output" + slash;

//         For Docker
        String[] command = {"/usr/bin/python3", EMPAD_HOME + "/pt/unpacker_out.py", tempSignalName, tempSignalOutFilePath, outFilePath};

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        Process process = processBuilder.start();

        System.out.println("Finalizing Results: " + signal);
        InputStream is = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
            if (line.contains(signal)) {
                System.out.println(signal + " took place into out folder");
                try {
                    FileUtils.deleteQuietly(new File(tempSignalOutFilePath + tempSignalName));
                } catch (Exception ignored) {
                }
                break;
            }
        }
    }
}
