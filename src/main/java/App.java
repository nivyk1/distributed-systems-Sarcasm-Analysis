public class App {
    final static AWS aws = AWS.getInstance();
    public static void main(String[] args) {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate, optional)]
        //* 1.Add a sanity check to each value
        // 2.check for length of args
        // 3. how does the terminate work?
        // *//
        String inFilePath = args[0];
        String outFilePath = args[1];
        int tasksPerWorker = Integer.parseInt(args[2]);

        try {
            setup();
            createEC2();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //Create Buckets, Create Queues, Upload JARs to S3
    private static void setup() {
        System.out.println("[DEBUG] Create bucket if not exist.");
        aws.createBucketIfNotExists(aws.bucketName);
    }

    private static void createEC2() {
        String ec2Script = "#!/bin/bash\n" +
                "echo Hello World\n";
        String managerInstanceID = aws.createEC2(ec2Script, "thisIsJustAString", 1);
    }
}
