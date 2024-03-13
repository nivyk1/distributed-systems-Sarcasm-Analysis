package org.example;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;



public class Manager {

    private static final String workerScript = "#! /bin/bash\n" +
            "AWS_ACCESS_KEY_ID="+System.getenv("AWS_ACCESS_KEY_ID")+
            "AWS_SECRET_ACCESS_KEY="+ System.getenv("AWS_SECRET_ACCESS_KEY")+
            "AWS_SESSION_TOKEN=" +System.getenv("AWS_SESSION_TOKEN")+"\n"+
            "sudo yum update -y\n" +
            "sudo yum install -y java-21-amazon-corretto\n" +
            "mkdir WorkerFiles\n" +
            "aws s3 cp s3://" + AWS.Jars_Bucket_name + "/worker.jar ./WorkerFiles\n" +
            "java -jar /WorkerFiles/worker.jar\n";
    final static AWS aws = AWS.getInstance();


    final static String input_Output_Bucket = AWS.input_Output_Bucket;
    private static String managerId;

    private static final String sqsFromclients = "clientsToManager.fifo";
    public static String clientsToManagerURL;
    private static final String sqsToClients = "managerToClients.fifo";
    public static String managerToClientsURL;
    private static final String sqsToWorkers = "managerToWorkers";
    public static String managerToWorkersURL;
    private static boolean terminateFlag = false;
    public static ConcurrentHashMap<String, String> workerIds = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, Integer> usersInputCount = new ConcurrentHashMap<>();





    public static void main(String[] args) {
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Manager started~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        setup();

        //stop receiving messages after terminate message
        while(!terminateFlag){
            ReceiveMessageFromClient();
        }
        System.out.println("received Terminate Message");
        //terminate all instances when finished
        while(processClientFile.getThreadCounter()>0){
            try {
                Thread.sleep(5000); //5 seconds sleep between everycheck
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("Entering terminate Function");
                terminate();

    }


    //message structure :"input"+"\t"+clientId + "\t" + inputPath+"\t"+batchesToUpload.size()+"\t"+tasksPerWorker+"\t"+totalreviews;
    private static void ReceiveMessageFromClient(){
        List<Message> messages = aws.receiveMessage(clientsToManagerURL, 1,400);

        if (!messages.isEmpty()) {
            System.out.println("Manager received a message");
            for (Message msg : messages) {
                if (msg.body().equals("terminate")) {
                    terminateFlag = true;
                    aws.deleteMessage(msg, clientsToManagerURL);
                    //don't receive more messages from clients
                    break;
                }
                else {
                    String[] msgArr =messages.get(0).body().split("\t");
                    int tasksPerWorker = Integer.parseInt(msgArr[4]);
                    int totalReviews = Integer.parseInt(msgArr[5]);
                    startWorkerInstances(tasksPerWorker,totalReviews);

                    String userId = msgArr[1];
                    updateUsersInputMap(userId);
                    int userInputCount = usersInputCount.get(userId);
                    try {

                       Thread thread=new Thread(new processClientFile(msg.body(),userInputCount));
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
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~created manager~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~entered setup~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        clientsToManagerURL = aws.getQueueUrl(sqsFromclients);
        managerToClientsURL = aws.getQueueUrl(sqsToClients);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~got client SQS~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        managerToWorkersURL = aws.createSQS(sqsToWorkers);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Created SQS~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        managerId = aws.getManagerId();
    }


    private static void terminate() {



        //Terminate all workers, then manager
        int numOfInstances=aws.countWorkerInstances();
        for (int i = 1; i <=numOfInstances; i++) {
            aws.terminateInstance(workerIds.get("worker" + i));
            workerIds.remove("worker"+i);
        }


        aws.deleteAllQueues(managerToClientsURL);
        aws.deleteAllBuckets();
        aws.terminateInstance(managerId);
    }







    public static class processClientFile implements Runnable {

        private static final Object counterLock = new Object();
        private static int threadCounter = 0;
        String clientId;
        String fileName;
        String fileNameWithoutSpecialChars;
        int userInputCount;
        int TotalNumOfBatches;
        int arrivedResults=0;;
        String [] results;
        String sqsUrl;



        public processClientFile(String msg,int _userInputCount) {
            userInputCount=_userInputCount; //we will use this to make the new SQS name
            String[] message = msg.split("\t");
            clientId=message[1];
            fileName=message[2];

            TotalNumOfBatches=Integer.parseInt(message[3]);
            results=new String[TotalNumOfBatches+1];
            IncrementThreadcounter();
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~thread started~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
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

            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"+"entered setup"+"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            sqsUrl= aws.createSQSFifo(clientId+"_"+userInputCount+".fifo");
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"+"sqs created"+"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        }

        //send the jobs to the queue of the workers according to the total num of batches
        public  void jobsToWorkersquere() {
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~jobs to worker~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            for (int i = 1; i <= TotalNumOfBatches; i++) {
                aws.sendMessageStandardQueue("input"+"\t"+clientId+"\t"+fileName+"\t"+userInputCount+"\t"+i, managerToWorkersURL);
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
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~thread started running~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

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
                    messages =aws.receiveMessage(sqsUrl, 1,400);
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
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~thread finished~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        }
    }

    //Initiating workers instances
    public static void startWorkerInstances(int tasksPerWorker, int totalReviews){

        int numOfWorkersNeeded = totalReviews/tasksPerWorker;
        int numberOfRunningWorkers = aws.countWorkerInstances();

        System.out.println("numberOfNeededWorkers-" + numOfWorkersNeeded);
        System.out.println("numberOfRunningWorkers-" + numberOfRunningWorkers);

        //keep uploading workers if another file arrived - until reached maximum for student capacity(9-manager)
        for (int i = 0; i < numOfWorkersNeeded; i++) {
            if (aws.countWorkerInstances() < AWS.MAX_WORKERS_INSTANCES) {
                String workerId = aws.createEC2(workerScript,"worker",1);
                String workerKey ="worker" + (i + numberOfRunningWorkers +1);
                workerIds.put(workerKey, workerId);
            }
            else{
                break;
            }
        }
    }

    public static void updateUsersInputMap(String userId){
        if(usersInputCount.containsKey(userId)){
            usersInputCount.put(userId,usersInputCount.get(userId) + 1);
        }
        else usersInputCount.put(userId,1);
    }

}



