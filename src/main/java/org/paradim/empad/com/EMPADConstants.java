package org.paradim.empad.com;

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
         @date: 05/23/2023
*/

public final class EMPADConstants {
    public final static String EMPAD_HOME = System.getenv("EMPAD_HOME");
//    public final static String EMPAD_HOME = "/Users/amir/empad";
    public final static int ERR_COMMAND = -1;

    /**** Map constants***/
    public final static int CHUNK_ID = 0;
    public final static int TOTAL_CHUNK = 1;

    public final static int SUBDIR_STR = 2;

    public final static int FILE_NAME = 3;
    public final static int DATA = 4;

    public final static int CHUNK_HASH = 5;
    public final static int PROTECTED_KEY_ID = 7;

    public final static String RAW_NAME = "scan_x256_y256.raw";

    public final static String NOISE_EXT = "bkg";
    public final static String OPERATION_EXT = ".xml";
    public final static String RAW_EXT = ".raw";
    public final static String PROTECTED_KEY = "EMAPD_EXP";

    public final static String[] EXCLUSIVE_VARS = {"even", "odd", ".tif", ".log", "stream_operations"};


    public final static String precision = "float32";
    /**** MessageUnpacker constants***/
    public final static int MSG_FILE_NAME = 0;
    public final static int MSG_CHUNK_HASH = 2;
    public final static int MSG_CHUNK_I = 4;
    public final static int MSG_N_TOTAL_CHUNKS = 5;
    public final static int MSG_SUBDIR_STR = 6;
    public final static int MSG_FILENAME_APPEND = 7;
    public final static int MSG_DATA = 8;

    public final static int UUID_LEN = 40;

}