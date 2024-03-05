import software.amazon.awssdk.services.ec2.model.InstanceType;

import java.io.File;
import java.util.UUID;

public class App {
    final static AWS aws = AWS.getInstance();
    static String clientId = UUID.randomUUID().toString();
    final static String bucketName = "input-bucket-nitay";
    public static String managerId;
    private static final String sqsOut = "clientsToManager";
    public static String clientsToManagerURL;
    private static final String sqsIn = "managerToClients";
    public static String managerToClientsURL;


    public static void main(String[] args) {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate, optional)]
        String inputFileName = args[0];
        String outputFileName = args[1];
        String tasksPerWorker = args[2];
//        boolean terminate = args.length > 3 && args[3].equals("terminate");

        try {
            setup(inputFileName,tasksPerWorker);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    //Create Buckets, Create Queues, Upload JARs to S3?
    private static void setup(String inputFileName, String tasksPerWorker) {
        activeManagerIfNotActive();
        //processRequest(inputFileName,tasksPerWorker);
        //System.out.println("[DEBUG] Create bucket if not exist.");
        //aws.createBucketIfNotExists("nitay-bucket-test");
    }


    //for test without using inputs through args
    private static void setup_test() {
        System.out.println("Creating SQS...");
        clientsToManagerURL = aws.createSQS(sqsOut);
        managerToClientsURL = aws.createSQS(sqsIn);
        System.out.println("Finished creating SQS");

        activeManagerIfNotActive();
        //processRequest(inputFileName,tasksPerWorker);
        //System.out.println("[DEBUG] Create bucket if not exist.");
        //aws.createBucketIfNotExists("nitay-bucket-test");
    }

    private static void activeManagerIfNotActive() {
        managerId = aws.checkIfManagerExist();
        if (managerId == null) {
            System.out.println("Creating SQS...");
            clientsToManagerURL = aws.createSQS(sqsOut);
            managerToClientsURL = aws.createSQS(sqsIn);
            System.out.println("Finished creating SQS");

            managerId=aws.createEC2Manager();
            System.out.println("Manager activated");
        } else
        {
            clientsToManagerURL = aws.getQueueUrl(sqsOut);
            managerToClientsURL = aws.getQueueUrl(sqsIn);
            System.out.println("Manager is already active");
        }
    }

    private static void processRequest(String inputFileName, String tasksPerWorker) {
        //put the input file in s3 storage
        aws.uploadFile(bucketName, clientId, new File(inputFileName));

        // notify the manager that it has a new task
        String message = clientId + "\t" + tasksPerWorker;
        aws.sendMessage(message, clientsToManagerURL);
    }

}
