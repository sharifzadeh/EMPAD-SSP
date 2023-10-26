package org.paradim.empad.com;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.Value;
import org.paradim.empad.dto.DataFileChunk;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;


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
         @update: 10/15/2023
*/

/**
 * DataFileChunkDeserializer
 */
public class DataFileChunkDeserializer extends AbstractDeserializationSchema<DataFileChunk> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    /**
     *
     * @param context
     */
    @Override
    public void open(InitializationContext context) {
        objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
    }

    /**
     * <p>This method deserializes the byte array message coming from the Kafka producer into a DataFileChunk object.
     * <a href="https://github.com/openmsi/openmsistream/blob/b92eeda583c529d64d38681172d552a1ca52de8b/openmsistream/kafka_wrapper/serialization.py#L178">DataFileChunkDeserializer</a>
     * To ensure that the data is correct, we compare the encoded binary values of both messages.
     * </p>
     *
     * @param message
     * @return DataFileChunk
     * @since 1.6
     */

    @Override
    public DataFileChunk deserialize(byte[] message) throws IOException {

        DataFileChunk dataFileChunk = new DataFileChunk();
        MessageDigest md = null;

        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(message);

        if (unpacker.hasNext()) {
            try {
                md = MessageDigest.getInstance("SHA-512");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            ImmutableValue value = unpacker.unpackValue();

            List<Value> list = value.asArrayValue().list();

            dataFileChunk.filename = String.valueOf(list.get(EMPADConstants.MSG_FILE_NAME));
            dataFileChunk.chunk_hash = list.get(EMPADConstants.MSG_CHUNK_HASH).asBinaryValue();

            dataFileChunk.chunk_i = Long.parseLong(String.valueOf(list.get(EMPADConstants.MSG_CHUNK_I)));
            dataFileChunk.n_total_chunks = Long.parseLong(String.valueOf(list.get(EMPADConstants.MSG_N_TOTAL_CHUNKS)));

            dataFileChunk.subdir_str = String.valueOf(list.get(EMPADConstants.MSG_SUBDIR_STR));

            dataFileChunk.filename_append = String.valueOf(list.get(EMPADConstants.MSG_FILENAME_APPEND));
            dataFileChunk.file_size = dataFileChunk.n_total_chunks;

            dataFileChunk.data = list.get(EMPADConstants.MSG_DATA).asBinaryValue();

            dataFileChunk.experiment = EMPADConstants.PROTECTED_KEY;

            assert md != null;

            md.update(dataFileChunk.data.asByteArray());
            byte[] bts = md.digest();

            String s1 = Base64.getEncoder().encodeToString(bts);
            String s2 = Base64.getEncoder().encodeToString(dataFileChunk.chunk_hash.asByteArray());

            if (!s1.equals(s2)) {
                try {
                    throw new Exception("chuck file did mot match with the hashed chunk!");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        return dataFileChunk;
    }
}
