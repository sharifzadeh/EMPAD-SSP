package org.paradim.empad.com;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/*
              #######      #         #       ##########          #            #########
              #            # #     # #       #        #         # #           #        #
              #            #  #   #  #       #        #        #   #          #         #
              #######      #   # #   #       ##########       #######         #          #
              #            #    #    #       #               #       #        #         #
              #            #         #       #              #         #       #        #
              ######       #         #       #             #           #      #########

         version 1.6.1
         @author: Amir H. Sharifzadeh, The Institute of Data Intensive Engineering and Science, Johns Hopkins University
         @date: 10/22/2023
*/

/**
 * <p>This class is implemented to receive signal and noise packets from the OpenMSI producer and then consume them locally in the associated directories.
 * No calculations are made here.
 * </p>
 */
public class EMPADConsumer extends ProcessFunction<Row, String> {

    @Override
    public void processElement(Row value, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {

    }
}
