import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 *
 * Test class for EMPAD's functionality (mostly corresponding computational functions in MATLAB)
 */
public class TestCase {

    private static final int chunkSizePower = 10;
    private static final String testPath = System.getProperty("user.dir");

    @Test
    public void testAddition() {
        assertEquals(2, sum(1, 1));
    }

    private int sum(int a, int b) {
        return a + b;
    }

    public static void main(String[] args) {
    }

    private void splitRawDataIntoChunks(String path) throws IOException {
        int fileLength = (int) new File(path).length();
        int chunkSize = (int) Math.pow(2, chunkSizePower);

        int bufferSize = fileLength / chunkSize;

        FileInputStream inputStream = new FileInputStream(path);
        byte[] buffer = new byte[bufferSize];

        String outPath;

        if (path.contains("bkg")) {
            outPath = testPath + "noise_chunks/";
        } else {
            outPath = testPath + "signal_chunks/";
        }

        int fileIndex = 1;
        while ((inputStream.read(buffer)) > 0) {
            FileUtils.writeByteArrayToFile(new File(outPath + fileIndex), buffer);
            fileIndex++;
        }

        inputStream.close();
    }

    @Test
    public void testReadSampleData() throws IOException {
        File file = new File(testPath + "/test/noise_chunks/1");
        assertTrue(file.exists());
    }

    @Test
    public void testReadSampleData2() throws IOException {
        File file = new File(testPath + "/test/noise_chunks/2assa");
        assertTrue(file.exists());
    }

    @Test
    public void testUnsignedUnpack() throws IOException {
    }
}