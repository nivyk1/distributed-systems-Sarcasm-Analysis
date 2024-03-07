package org.example;
import org.json.*;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class Manager {
    final static AWS aws = AWS.getInstance();

    private static final String sqsFromclients = "clientsToManager";
    public static String clientsToManagerURL;
    private static final String sqsToClients = "managerToClients";
    public static String managerToClientsURL;
    private static final String sqsFromWorkers = "workersToManager";
    public static String workersToManagerURL;
    private static final String sqsToWorkers = "managerToWorkers";
    private static boolean terminateFlag = false;
    public static ConcurrentHashMap<String, Integer> urlCountPerClient = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, String> workerIds = new ConcurrentHashMap<>();
    public static String managerToWorkersURL;
    public static void main(String[] args) {
        setup();
        //stop receiving messages after terminate message
        if(!terminateFlag)
            ReceiveMessage();
    }

    private static void ReceiveMessage(){
        List<Message> messages = aws.receiveMessage(clientsToManagerURL, 1);
        if (messages.size() > 0) {
            System.out.println("Manager received a message");
        }
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
                    System.out.println(e.getMessage());
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
        String[] messages = msg.split("\t");

        if(messages.length >= 2) {
            String clientId = messages[0];
            runningTasks.add(clientId);
            int n = Integer.parseInt(messages[1]);
            InputStream task = aws.getFile(bucketName,clientId);
            try {
                System.out.println("** handling " + clientId);
                String message = convertInputStreamToString(task);
                aws.deleteFile(bucketName, clientId);
                String[] urls = message.split("\n");
                urlCountPerClient.put(clientId, urls.length);
                int numOfWorkersNeeded = Math.min((urls.length / n), 16);

                int numberOfRunningWorkers = aws.getNumberofRunningWorkers();
                System.out.println("numberOfNeededWorkers-" + numOfWorkersNeeded);
                System.out.println("numberOfRunningWorkers-" + numberOfRunningWorkers);
                for (int i = numberOfRunningWorkers; i < numOfWorkersNeeded; i++) {
                    if (aws.getNumberofRunningWorkers() < 15) {
                        String id = aws.createWorker(i);
                        workerIds.put("worker" + i, id);
                    }
                }

                for (String url : urls) {
                    aws.sendMessage(clientId + "\t" + url, managerToWorkersURL);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("~~~finished sending messages~~~");
            }
        }
    }


    //parsing input file into batches
    // add counter to each batch in order to get total
    public List<String>  jsonparser(String path) throws FileNotFoundException {

        List<String> reviewsList = new ArrayList<String>();

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Create a JSONTokener with the JSON string
                JSONTokener tokener = new JSONTokener(line); // Create a JSONObject using the JSONTokener
                JSONObject jsonObject = new JSONObject(tokener);
                String reviewsString="";

                // Access the values of the keys
                JSONArray reviewsArray = jsonObject.getJSONArray("reviews");
                for(int i=0;i<reviewsArray.length();i++) {
                    JSONObject reviewObject = reviewsArray.getJSONObject(i);
                   reviewsString=reviewsString + reviewObject.getString("link")+" "+ reviewObject.getString("text")+" "+ reviewObject.getInt("rating")+ "\n";
                }

                reviewsList.add(reviewsString);
            }

            return reviewsList;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }
}



