package org.example;

import com.google.protobuf.Internal;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.File;
import java.util.HashMap;
import java.util.List;
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
    HashMap<String, StringBuilder> map = new HashMap<>();


    public static void main(String[] args) {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate, optional)]
       // String inputFileName = args[0];
      //  String outputFileName = args[1];
      //  String tasksPerWorker = args[2];
      // boolean terminate = args.length > 3 && args[3].equals("terminate");
        boolean terminate = args[args.length-1].equals("terminate");
       String tasksPerWorker= terminate ? args[args.length-2]: args[args.length-1];
        int numOfInputFiles=argsCheck(args,terminate);
        //TODO if numOfInputFiles ==-1 raise error

        managerId = aws.checkIfManagerExist();
        if(managerId==null)
        {
            try {
                setup();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else
        {
            clientsToManagerURL = aws.getQueueUrl(sqsOut);
            managerToClientsURL = aws.getQueueUrl(sqsIn);
        }

        //todo maybe need to check if it uploaded succsessfully
        uploadInputstoS3(args,numOfInputFiles,tasksPerWorker);

        // loop for waiting for results
        int numOfOutputs=0;
       boolean isfinished=false;
       while(!isfinished)
       {
           List<Message> messages = aws.receiveMessage(managerToClientsURL, 5);
           for (Message m:messages)
           {
               //messageformat(maybe?) clientID+inputfilename+ rest of lines from the form <p>something</p>\n<p>something</p>\n<p>something</p>
               numOfOutputs++;
               //todo createhtml(m)

           }


       }





    }

    //Create Buckets, Create Queues
    private static void setup() {
        System.out.println("[DEBUG] Create bucket if not exist.");
        aws.createBucketIfNotExists("niv-aws-test");
        activeManagerIfNotActive();
        //processRequest(inputFileName,tasksPerWorker);

    }

    //checks if the number of input file equals to the number of output file
    //and return the number of inputfiles. if its not equal, return -1;
    public static int argsCheck(String []args,boolean terminate)
    {
        if(terminate)
        {
            if (args.length %2==0) {
                return (args.length - 2) / 2;
            }
            else
            {
                return -1;
            }
        }

        else
        {
            if (args.length %2==1) {
                return (args.length - 1) / 2;
            }
            else
            {
                return -1;
            }
        }
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

    // todo maybe need to add try and catch statements
    private static void uploadInputstoS3(String [] args,int numOfInputFiles,String tasksPerWorker)
    {
        for(int i=0;i<numOfInputFiles-1;i++)
        {
            aws.uploadFile(bucketName, clientId, new File(args[i]));
            // notify the manager that it has a new task
            String message = clientId + "\t" + tasksPerWorker;
            aws.sendMessage(message, clientsToManagerURL);

        }
    }

}
