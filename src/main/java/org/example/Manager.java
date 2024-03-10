package org.example;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class Manager {

    private static final String workerScript = "#! /bin/bash\n" +
            "sudo yum update -y\n" +
            "sudo yum install -y java-21-amazon-corretto\n" +
            "mkdir WorkerFiles\n" +
            "aws s3 cp s3://" + AWS.Jars_Bucket_name + "/worker.jar ./WorkerFiles\n" +
            "java -jar /WorkerFiles/worker.jar\n";
    final static AWS aws = AWS.getInstance();


    final static String input_Output_Bucket = AWS.input_Output_Bucket;
    
    private static final int reviewPerBatch = 10;
    private static String managerId;

    private static final String sqsFromclients = "clientsToManager";
    public static String clientsToManagerURL;
    private static final String sqsToClients = "managerToClients";
    public static String managerToClientsURL;
    private static final String sqsFromWorkers = "workersToManager";
    public static String workersToManagerURL;
    private static final String sqsToWorkers = "managerToWorkers";
    public static String managerToWorkersURL;
    private static boolean terminateFlag = false;

    public static ConcurrentHashMap<String, String> workerIds = new ConcurrentHashMap<>();





    public static void main(String[] args) {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Manager started~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        setup();

        //stop receiving messages after terminate message
        while(!terminateFlag){
            ReceiveMessageFromClient();
        }
        //terminate all instances when finished
        while(processClientFile.getThreadCounter()>0){
            try {
                Thread.sleep(5000); //5 seconds sleep between everycheck
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
            terminate();

    }

    private static void ReceiveMessageFromClient(){
        List<Message> messages = aws.receiveMessage(clientsToManagerURL, 1);
        if (!messages.isEmpty()) {
            System.out.println("Manager received a message");
            for (Message msg : messages) {
                if (msg.body().equals("terminate")) {
                    terminateFlag = true;
                    //don't receive more messages from clients
                    break;
                }
                else {
                    try {

                       Thread thread=new Thread(new processClientFile(msg.body()));
                       thread.start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        aws.deleteMessage(msg, clientsToManagerURL);
                    }
                }
            }
        }

    }


    //Setting up SQS and other resources
    private static void setup(){
        //managerId = aws.createEC2Manager();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~created manager~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~entered setup~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        clientsToManagerURL = aws.getQueueUrl(sqsFromclients);
        managerToClientsURL = aws.getQueueUrl(sqsToClients);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~got client SQS~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        managerToWorkersURL = aws.getQueueUrl(sqsToWorkers);
        workersToManagerURL = aws.getQueueUrl(sqsFromWorkers);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Created SQS~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        managerId = aws.getManagerId();
    }


    private static void terminate() {


        aws.deleteAllQueues();
        aws.deleteAllBuckets();

        //Terminate all workers, then manager
        for (int i = 0; i < aws.countWorkerInstances(); i++) {
            aws.terminateInstance(workerIds.get("worker" + i +1));
        }
        aws.terminateInstance(managerId);
    }







    public static class processClientFile implements Runnable {

        private static Object counterLock = new Object();
        private static int threadCounter = 0;
        String clientId;
        String fileName;
        int TotalNumOfBatches;
        int arrivedResults=0;;
        String [] results;
        String sqsUrl;



        public processClientFile(String msg) {
            String[] message = msg.split("\t");
            clientId=message[1];
            fileName=message[2];
            TotalNumOfBatches=Integer.parseInt(message[3]);
            results=new String[TotalNumOfBatches+1];
            IncrementThreadcounter();
        }
        public static  void IncrementThreadcounter() {
            synchronized (counterLock) {
                threadCounter++;
            }
        }
        public static void DecrementThreadcounter() {
            synchronized (counterLock) {
                threadCounter--;
            }
        }

        public static int getThreadCounter() {
            synchronized (counterLock) {
                return  threadCounter;
            }
        }


        public void mysetup()
        {
            aws.createSQS(sqsFromWorkers+clientId+fileName);
            sqsUrl= aws.getQueueUrl(sqsFromWorkers+clientId+fileName);
        }

        //send the jobs to the queue of the workers according to the total num of batches
        public  void jobsToWorkersquere() {
            for (int i = 1; i <= TotalNumOfBatches; i++) {
                aws.sendMessage("input"+"\t"+clientId+"\t"+fileName+"\t"+i, managerToWorkersURL);
            }
        }

        //gets the batchnumber from the worker downloads it parse it into String and put the String in the right place the result Array
        public  void messagehandler(Message msg) throws IOException {
           int batchResultnumber = Integer.parseInt(msg.body());
            InputStream workerOutput = aws.getFile(input_Output_Bucket,"output"+"\t"+clientId+"\t"+fileName+"\t"+batchResultnumber);
            StringBuilder batchResult = new StringBuilder();

            BufferedReader reader = new BufferedReader(new InputStreamReader(workerOutput));
            String line;
            while ((line = reader.readLine()) != null) {
                batchResult.append(line).append("\n");
            }

             results[batchResultnumber]=batchResult.toString();
            arrivedResults++;


        }

        //merge all the results and uploads the output to S3
        public void finalResult()
        {
            StringBuilder finalresult=new StringBuilder();

            for (int i=1;i<=TotalNumOfBatches;i++ ) {

                finalresult.append(results[i]);

            }

            aws.uploadString(input_Output_Bucket,"output"+"\t"+clientId+"\t"+fileName,finalresult.toString());
            aws.sendMessage("output"+"\t"+clientId+"\t"+fileName,sqsToClients);



        }


        @Override
        public void run() {

            boolean isDone=false;

            mysetup();

            jobsToWorkersquere();

            while(!isDone){
                List<Message> messages;
                do {
                    try {
                        Thread.sleep(100); //wait 0.1 second before checking
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    messages =aws.receiveMessage(workersToManagerURL, 1);
                }while (messages.isEmpty());

                try {
                    messagehandler(messages.get(0));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    aws.deleteMessage(messages.get(0),sqsUrl);
                }

                if(arrivedResults==TotalNumOfBatches)
                {
                    isDone=true;
                }

            }

            finalResult();
            aws.deleteSingleQueue(sqsUrl);
            processClientFile.DecrementThreadcounter();

        }
    }

}



