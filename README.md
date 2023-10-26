## General Specification:

EMPAD-SSP (Electron Microscope Pixel Array Detector - Streaming Signal Processing) is an extended version of the standalone Python/Cython that was implemented for paired noise and signal processing. The ultimate goal is streaming data processing. This means that any data sent by the Kafka platform if it passes the required conditions (please read the Producer Side), will be processed without interruption and priority, and the results will be applied to other computing services at the appropriate time. 

In the previous version, each pair of signal and noise data was read from a local system and processed to create a corrected image.

In this program, signal and noise data are streamed through the Kafka connector ([OpenMSIStream](https://openmsistream.readthedocs.io/en/latest/)) in the form of scattered packets and processed simultaneously.


The processor service we have used is [Apache Flink (1.17.1)](https://flink.apache.org/), which is a stateful streaming platform and has a [relatively better performance than Spark](https://www.macrometa.com/event-stream-processing/spark-vs-flink) for stream data processing.

In addition, we have stored some results of processed data (such as noise) in the memory, which will be served to other processed signals later, so that the program has a higher speed.

On the other hand, all filter data (eight filters in total) are also stored in a [MATLAB file](https://github.com/paradimdata/varimatstream/blob/main/mask/mask.mat) and evaluated by an open-source Java library ([JMatIO](https://github.com/diffplug/JMatIO)) during program execution.

We also tried to minimize redundant calculations and focus on performance and accuracy.

## Technical Specification:

## General workflow processes:
1. [Authentication](https://github.com/paradimdata/varimatstream/blob/main/src/main/resources/META-INF/main/java/org/paradim/empad/com/EMPADStreamCommand.java#L147)
2. [Deserialization](https://github.com/paradimdata/varimatstream/blob/main/src/main/resources/META-INF/main/java/org/paradim/empad/com/DataFileChunkDeserializer.java#L52)
3. [Filter Extraction](https://github.com/paradimdata/varimatstream/blob/main/src/main/resources/META-INF/main/java/org/paradim/empad/com/StreamingSignalProcessing.java#L101)
4. [Query Investigation](https://github.com/paradimdata/varimatstream/blob/main/src/main/resources/META-INF/main/java/org/paradim/empad/com/EMPADStreamCommand.java#L197)
5. [State Registration](https://github.com/paradimdata/varimatstream/blob/main/src/main/resources/META-INF/main/java/org/paradim/empad/com/StreamingSignalProcessing.java#L357)
6. [Computational Signal Processing](https://github.com/paradimdata/varimatstream/blob/main/src/main/resources/META-INF/main/java/org/paradim/empad/com/StreamingSignalProcessing.java#L720) and [State Verification](https://github.com/paradimdata/varimatstream/blob/main/src/main/resources/META-INF/main/java/org/paradim/empad/com/StreamingSignalProcessing.java#L269)
7. [Debouncing](https://github.com/paradimdata/varimatstream/blob/main/src/main/resources/META-INF/main/java/org/paradim/empad/com/StreamingSignalProcessing.java#L834)
8. [Transforming Filters](https://github.com/paradimdata/varimatstream/blob/main/src/main/resources/META-INF/main/java/org/paradim/empad/com/StreamingSignalProcessing.java#L936), and [Frame Corrections](https://github.com/paradimdata/varimatstream/blob/main/src/main/resources/META-INF/main/java/org/paradim/empad/com/StreamingSignalProcessing.java#L1019)



## Producer Side:
1. Each directory must contain the noise, signal, or signals, and a corresponding XML file.
(TODO: We may implement an application to verify those files).
2. To distinguish the noise file from the signal files, we agreed that the name of each noise file ends with [_bkg](https://github.com/paradimdata/varimatstream/blob/main/src/main/resources/META-INF/main/java/org/paradim/empad/com/EMPADConstants.java)
3. The name of each XML file inside the directory (regardless of the extension) must be exactly the same as the directory name. We use this XML file so that it can be distinguished when executing a query on other signals that are dependent on other noise. This XML file also contains information about the dimensions of the frames and their measurement accuracy, which may be different in different tests, which will directly impact our calculations.
4. The producer must be only conducted through the [DataFileUploadDirectory](https://openmsistream.readthedocs.io/en/latest/user_info/main_programs/data_file_upload_directory.html)

   
## Dockerization:
1. **Installing Docker**: You need to make sure that [Docker](https://docs.docker.com) is already installed on your computer and is running. Follow the [instruction](https://docs.docker.com/engine/install/) on how to install docker on your operating system.
2. **Building the docker image**: From the terminal run this command line `docker build -t openmsi/empad:1.6 .`. **1.6** is the latest version at this point.
3. **Modifying the environment file**: the .env file contains the following data and needs to be modified before running the image:
 3.1. _EMPAD_HOME_: The root directory of the application (i.e. EMPAD_HOME=/empad)
 3.2 _KAFKA_ENV_USERNAME_, KAFKA_ENV_PASSWORD, _EMPAD_TOPIC_ and _GROUP_ID_ need to be defined correctly in your environment.
4. **Running the image: From terminal run this command**: `docker run --env-file ./.env -v ./out:/empad/output` where corrected images take place into the **output** directory.
