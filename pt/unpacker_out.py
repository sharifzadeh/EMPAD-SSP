import os
import struct
import sys

import numpy as np


#      #######      #         #       ##########          #            #########
#      #            # #     # #       #        #         # #           #        #
#      #            #  #   #  #       #        #        #   #          #         #
#      #######      #   # #   #       ##########       #######         #          #
#      #            #    #    #       #               #       #        #         #
#      #            #         #       #              #         #       #        #
#      ######       #         #       #             #           #      #########
#
# version 1.6
# @author: Amir H. Sharifzadeh, The Institute of Data Intensive Engineering and Science, Johns Hopkins University
# @date: 06/14/2023
# @last modified: 07/06/2023

def do_unpack(temp_signal_name, temp_signal_path, out_signal_path):
    signal_name = 'err'

    try:
        fid = open(str(temp_signal_path) + str(temp_signal_name), 'rb')
        fid.seek(0, os.SEEK_END)
        nFileLen = fid.tell()
        fid.seek(0, 0)

        chunk_size = 4

        nLenVals = round(nFileLen / 4)
        nFrames = round(nLenVals / 128 / 128)

        signal_name = str(temp_signal_name[5:])

        npFrames = np.zeros((nFrames, 128, 128), dtype='float32')
        with open(str(out_signal_path + signal_name), 'wb') as f:
            for i in range(0, nFrames):
                nLenVals_i = round(chunk_size / 4)
                nVals_i = struct.unpack('>' + 'f' * nLenVals_i * 128 * 128, fid.read(chunk_size * 128 * 128))
                arr = np.array(nVals_i)
                arr2 = arr.reshape(128, 128)
                npFrames[i] = arr2
                f.write(npFrames[i])

            fid.close()

            f.flush()
            f.close()
    except:
        print("An exception occurred")

    return signal_name

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("wrong arguments!")
        exit()

    args = sys.argv[1:]
    temp_signal_name = args[0]

    temp_signal_path = args[1]

    out_signal_path = args[2]

    if not os.path.exists(str(temp_signal_path) + str(temp_signal_name)):
        print('the signal path was not valid!')
        exit()
    signal_name = do_unpack(temp_signal_name, temp_signal_path, out_signal_path)
    print(signal_name)
