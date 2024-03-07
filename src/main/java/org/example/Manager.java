package org.example;
import org.json.*;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class Manager {
    //todo change the jar accordingly
    private static final String workerScript = "#! /bin/bash\n" +
            "sudo yum update -y\n" +
            "sudo yum install -y java-21-amazon-corretto\n" +
            "mkdir WorkerFiles\n" +
            "aws s3 cp s3://" + AWS.Jars_Bucket_name + "/worker.jar ./WorkerFiles\n" +
            "java -jar /WorkerFiles/worker.jar\n";
    final static AWS aws = AWS.getInstance();
    final static String inputBucket = "inputobjects";

    private static final String sqsFromclients = "clientsToManager";
    public static String clientsToManagerURL;
    private static final String sqsToClients = "managerToClients";
    public static String managerToClientsURL;
    private static final String sqsFromWorkers = "workersToManager";
    public static String workersToManagerURL;
    private static final String sqsToWorkers = "managerToWorkers";
    private static boolean terminateFlag = false;
    public static ConcurrentHashMap<String, Integer> filesPerClient = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, Integer> reviewsPerFile = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, String> workerIds = new ConcurrentHashMap<>();

    private static List<String> MessagesInProgress = new LinkedList<>();
    public static String managerToWorkersURL;
    public static void main(String[] args) {
        setup();
        //stop receiving messages after terminate message
        if(!terminateFlag)
            ReceiveMessage();
    }

    private static void ReceiveMessage(){
        List<Message> messages = aws.receiveMessage(clientsToManagerURL, 1);
        if (!messages.isEmpty()) {
            System.out.println("Manager received a message");
        }
        //todo , add else in case list is empty
        for (Message msg : messages) {
            if (msg.body().equals("terminate")) {
                terminateFlag = true;
                //don't receive more messages
                break;
            }
            else {
                try {
                    handleMessage(msg.body());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    aws.deleteMessage(msg, clientsToManagerURL);
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
    }


    private static void handleMessage(String msg) {
        System.out.println("Manager handling task");
        String[] messages = msg.split("\t"); // Message structure ClientId/nfilename/tasksperworker
        String fileName = messages[0]; //Structure - ClientId/nfilename
        int tasksPerWorker = Integer.parseInt(messages[1]);
        MessagesInProgress.add(fileName);

        InputStream task = aws.getFile(inputBucket,fileName);
        try {
            System.out.println("starting work on " + fileName);
            List<String> reviewList = jsonparser(task,fileName); //gets a list of reviews each string contains one line of reviews
            //aws.deleteFile(bucketName, clientId);

            int numOfWorkersNeeded = reviewsPerFile.get(fileName)/tasksPerWorker;
            int numberOfRunningWorkers = aws.countWorkerInstances();

            System.out.println("numberOfNeededWorkers-" + numOfWorkersNeeded);
            System.out.println("numberOfRunningWorkers-" + numberOfRunningWorkers);
            for (int i = 0; i < numOfWorkersNeeded; i++) {
                if (aws.countWorkerInstances() < 8) {
                    String workerId = aws.createEC2(workerScript,"worker",1);
                    workerIds.put("worker" + i + numberOfRunningWorkers + 1, workerId);
                }
                else{
                    break;
                }
            }

            for (String review : reviewList) {
                aws.sendMessage(fileName + "\t" + review, managerToWorkersURL);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("~~~finished sending messages~~~");
        }

    }


    //parsing input file into batches
    // add counter to each batch in order to get total
    public static List<String> jsonparser(InputStream file,String fileName) throws FileNotFoundException {

        List<String> reviewsList = new ArrayList<String>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file))) {
            String line;
            int reviewcount=0;
            while ((line = reader.readLine()) != null) {
                // Create a JSONTokener with the JSON string
                JSONTokener tokener = new JSONTokener(line); // Create a JSONObject using the JSONTokener
                JSONObject jsonObject = new JSONObject(tokener);
                String reviewsString="";

                // Access the values of the keys
                JSONArray reviewsArray = jsonObject.getJSONArray("reviews");
                for(int i=0;i<reviewsArray.length();i++) {
                    reviewcount++;
                    JSONObject reviewObject = reviewsArray.getJSONObject(i);
                   reviewsString=reviewsString + reviewObject.getString("link")+" "+ reviewObject.getString("text")+" "+ reviewObject.getInt("rating")+ "\n";
                }
                reviewsPerFile.put(fileName,reviewcount);
                reviewsList.add(reviewsString);
            }


            return reviewsList;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}



