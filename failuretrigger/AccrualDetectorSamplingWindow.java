package siemens.db.adapt.FailureDetection.failuretrigger;

import org.apache.commons.math3.distribution.NormalDistribution;

import java.lang.reflect.Array;

public class AccrualDetectorSamplingWindow {

    private long[] samplingWindow;
    private int windowSize;
    private int arrayIndex;
    private int sampleCount;

    AccrualDetectorSamplingWindow(int windowSize) {
        this.windowSize = windowSize;
        this.sampleCount = 0;
        samplingWindow = new long[windowSize];
        arrayIndex = 0;
    }

    void addNewSample(long sample) {
        samplingWindow[arrayIndex] = sample;

        //System.out.println("Adding sample " + sample + " at index " + arrayIndex);
        arrayIndex++;
        if (arrayIndex >= windowSize)
            arrayIndex = 0;

        if (sampleCount<windowSize)
            sampleCount++;
    }

    boolean isSamplingWindowFull() {
        if (sampleCount >= windowSize)
            return true;
        else
            return false;
    }

    double phiValue(Long lastTimestamp) {
        if (isSamplingWindowFull()) {
            // compute mean and standard deviation
            long sumOfSquares = 0;
            double sum = 0;
            // TODO: optimise. no need for 'for' loop
            for (long sample : samplingWindow) {
                sum += sample;
                sumOfSquares += sample * sample;
            }
            double squareOfSum = sum * sum;


            double sd = Math.sqrt((sumOfSquares - squareOfSum / samplingWindow.length) / (samplingWindow.length - 1));
            if (sd<=0.0) {
                System.out.println("sumOfSq: " + sumOfSquares + "; sqOfSum: " + squareOfSum + "; sampling window length: " + samplingWindow.length);
                for (long sample : samplingWindow)
                    System.out.println(sample);
            }
            NormalDistribution d = new NormalDistribution(sum / samplingWindow.length,
                    sd);


            // return phi
            return (-1.0 * Math.log10(1.0 - d.cumulativeProbability(System.currentTimeMillis() - lastTimestamp)));
        }

        // return 0 if sampling window is not full yet
        return 0;
    }
}
