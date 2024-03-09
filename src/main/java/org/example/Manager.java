package org.example;
import org.json.*;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class Manager {
    //todo change the jar accordingly
    private static final String workerScript = "#! /bin/bash\n" +
            "sudo yum update -y\n" +
            "sudo yum install -y java-21-amazon-corretto\n" +
            "mkdir WorkerFiles\n" +
            "aws s3 cp s3://" + AWS.Jars_Bucket_name + "/worker.jar ./WorkerFiles\n" +
            "java -jar /WorkerFiles/worker.jar\n";
    final static AWS aws = AWS.getInstance();

    //todo decide on a name for global bucket
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
    public static ConcurrentHashMap<String, Integer> fileBatchCounter = new ConcurrentHashMap<>(); //key = file name , value = counter
    public static ConcurrentHashMap<String, Integer> totalBatchesPerFile = new ConcurrentHashMap<>();//key = file name , value = TotalBatch
    public static ConcurrentHashMap<String, String> workerIds = new ConcurrentHashMap<>();





    public static void main(String[] args) {
        setup();
        // opening a new thread that receive messages from worker and uploads full output when needed
        RecieveMessage recieveMessages = new RecieveMessage();
        Thread recieveMessagesThread = new Thread(recieveMessages);
        recieveMessagesThread.start();

        //stop receiving messages after terminate message
        while(!terminateFlag){
            ReceiveMessageFromClient();
        }
        //terminate all instances when finished
        while(!fileBatchCounter.isEmpty()){
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
                        handleClientMessage(msg.body());
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
        clientsToManagerURL = aws.getQueueUrl(sqsFromclients);
        managerToClientsURL = aws.getQueueUrl(sqsToClients);
        managerToWorkersURL = aws.createSQS(sqsToWorkers);
        workersToManagerURL = aws.createSQS(sqsFromWorkers);
        managerId = aws.checkIfManagerExist(); //use this function to get manager id
    }


    private static void handleClientMessage(String msg) {
        System.out.println("Manager handling task");
        String[] messages = msg.split("\t"); // Message structure input or output(this function handles client msg so it will be input) /t ClientId/t filename/t totalBatch /t tasksperworker /t Totalreviews
        String input = messages[0];
        String clientId = messages[1];
        String fileName = messages[2];
        int totalBatch = Integer.parseInt(messages[3]);
        int tasksPerWorker = Integer.parseInt(messages[4]);
        int totalReviews = Integer.parseInt(messages[5]);

        String key = input + "\t" + clientId + "\t" + fileName;

        //initiating new file in map with the value 0, when all the total rows finished we will process the full file
        fileBatchCounter.put(fileName,0);
        totalBatchesPerFile.put(fileName,totalBatch);

        try {
            System.out.println("starting work on " + fileName);
            
            //Initiating workers instances 
            int numOfWorkersNeeded = (totalBatch*reviewPerBatch)/tasksPerWorker;
            
            //todo check that this function is working
            int numberOfRunningWorkers = aws.countWorkerInstances();

            System.out.println("numberOfNeededWorkers-" + numOfWorkersNeeded);
            System.out.println("numberOfRunningWorkers-" + numberOfRunningWorkers);
            
            //keep uploading workers if another file arrived - until reached maximum for student capacity(9-manager)
            for (int i = 0; i < numOfWorkersNeeded; i++) {
                if (aws.countWorkerInstances() < AWS.MAX_WORKERS_INSTANCES) {
                    String workerId = aws.createEC2(workerScript,"worker",1);
                    workerIds.put("worker" + i + numberOfRunningWorkers + 1, workerId);
                }
                else{
                    break;
                }
            }
            //send msg to worker for each batch
            for (int i = 1; i < totalBatch + 1; i++) {
                aws.sendMessage(key + "\t" +i ,managerToWorkersURL);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("~~~finished sending messages~~~");
        }

    }

    private static void terminate() {
        RecieveMessage.terminate = true;
        aws.deleteAllQueues();

        aws.deleteAllBuckets();

        //Terminate all workers, then manager
        for (int i = 0; i < aws.countWorkerInstances(); i++) {
            aws.terminateInstance(workerIds.get("worker" + i +1));
        }
        aws.terminateInstance(managerId);
    }


    public static class RecieveMessage implements Runnable {
        public static boolean terminate = false;

        //Count all returning batches from a file
        @Override
        public void run() {
            while(!terminate){
                List<Message> messages;
                do {
                    try {
                        Thread.sleep(100); //wait 0.1 second before checking
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    messages =aws.receiveMessage(workersToManagerURL, 1);
                }while (messages.isEmpty());

                handleWorkerMessage(messages.get(0));
            }
        }
        public void handleWorkerMessage(Message msg){
            String[] message = msg.body().split("\t"); //Structure outPut \t UID \t Filename \t row
            String outPut = message[0];
            String clientId = message[1];
            String fileName = message[2];
            String key = outPut + "\t" + clientId + "\t" + fileName;
            int currCount = fileBatchCounter.get(fileName);
            fileBatchCounter.put(fileName,currCount+1);

            //successfully remove from queue --> the message in the sqs considered "inflight" until the visibility time finishes
            try{
                aws.deleteMessage(msg,workersToManagerURL);
            }
            catch (Exception e){
                throw new RuntimeException(e);
            }

            //Process full input file when all workers are finished
            if ((fileBatchCounter.get(fileName)).equals (totalBatchesPerFile.get(fileName))){

                //option to open a thread for the function processInputFile
                processInputFile(key,fileName);
                //notify client that the file is ready! the key is the path in s3
                aws.sendMessage(key,managerToClientsURL);
                removeFromMap(fileName);
            }
        }

        public void processInputFile(String key,String filename){
            try {
                StringBuilder MergedFiles = new StringBuilder ();
                for (int i = 1; i < totalBatchesPerFile.get(filename)+1; i++) {
                    String batchKey = key +"\t"+i;
                    InputStream batch = aws.getFile(input_Output_Bucket,batchKey);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(batch));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        MergedFiles.append(line).append("\n");
                    }
                }
                //Upload to S3 completed output!!!
                aws.uploadString(input_Output_Bucket,key,MergedFiles.toString());

            }
            catch(Exception e){
                e.printStackTrace();
            }
        }

        public void removeFromMap(String fileName){
            fileBatchCounter.remove(fileName);
            totalBatchesPerFile.remove(fileName);
        }

    }

}



