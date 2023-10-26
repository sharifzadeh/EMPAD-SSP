package org.paradim.empad.dto;

import org.apache.flink.api.java.tuple.Tuple2;

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
         @date: 04/03/2023
*/

public class OperationTO {

    private Tuple2<Integer, Integer> sensor;
    private int framecount;
    private String datatype;
    private String filename;

    public OperationTO() {
    }

    public Tuple2<Integer, Integer> getSensor() {
        return sensor;
    }

    public void setSensor(Tuple2<Integer, Integer> sensor) {
        this.sensor = sensor;
    }

    public int getFramecount() {
        return framecount;
    }

    public void setFramecount(int framecount) {
        this.framecount = framecount;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }
}
