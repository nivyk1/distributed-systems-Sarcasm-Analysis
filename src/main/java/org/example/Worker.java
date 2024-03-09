package org.example;

import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.List;

public class Worker {
    private static sentimentAnalysisHandler sentimenthandler=new sentimentAnalysisHandler();
   private static namedEntityRecognitionHandler entityhandler=new namedEntityRecognitionHandler();

   private static final String[] colors = {
            "dark red",
            "red",
            "black",
            "light green",
            "dark green"
    };
    private static final String sqsToManager = "workersToManager";
    public static String workersToManagerURL;
    private static final String sqsFromManager = "ManagerToWorkers";
    public static String ManagerToWorkersURL;
    private final static AWS aws = AWS.getInstance();
    private final static String bucketName = "inputobjects";

    public static boolean isTerminated=false;
// "input"+"\t"+Uid"+"\t"+Filename+"rownumber"
    public Worker() {

    }
    public static void main(String[] args) throws IOException {

        while (true) {
            List<Message> messages = aws.receiveMessage(ManagerToWorkersURL, 1);
            for (Message msg : messages) {
                if (msg.body().equals("terminate")) {
                    isTerminated = true;
                    //don't receive more messages
                    break;
                } else {
                    String outPutKey="output"+"\t"+msg.body().split("\t",2)[1];
                    //process the job and send the result to the queue
                    aws.uploadString(bucketName,outPutKey,jobProccess(aws.getFile(bucketName,msg.body())));
                    // notify the manager that it has a new task
                    String message = outPutKey;
                    aws.sendMessage(message, workersToManagerURL);
                }
            }

        }
    }
    public static String jobProccess(InputStream job) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(job))) {
            String line;
            StringBuilder result= new StringBuilder();
            while ((line = reader.readLine()) != null) {

                result.append(ReviewHtmlmaker(line.split("niv")));


        }
            return result.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }


    // get the review and return a single line of the requested  output format for this specific review.
    public static String ReviewHtmlmaker(String[] review) {
        StringBuilder result = new StringBuilder();
        int sentiment = sentimenthandler.findSentiment(review[1]);
        result = new StringBuilder("<p><a href=\"" + review[0] + "\" " + "style=\"color: " + colors[sentiment] + ";\">link</a>\t[");
        List<String> reviewEntities = entityhandler.findEntities(review[1]);
        if (!reviewEntities.isEmpty()) {
            for (String s : reviewEntities) {
                result.append(s).append(",");
            }
            result = new StringBuilder(result.substring(0, result.length() - 1));
        }

        result.append("]");

        if (Integer.parseInt(review[2]) == sentiment) {
            result.append("\t no sarcasm</p>\n");
        } else {
            result.append("\t sarcasm</p>\n");
        }


        return result.toString();

    }

    }



