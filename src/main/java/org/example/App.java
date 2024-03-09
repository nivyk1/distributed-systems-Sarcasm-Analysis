package org.example;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class App {
    final static AWS aws = AWS.getInstance();
    static String clientId = UUID.randomUUID().toString();
    final static String bucketName = "SuperDuperBucket";
    public static String managerId;
    private static final String sqsOut = "clientsToManager";
    public static String clientsToManagerURL;
    private static final String sqsIn = "managerToClients";
    public static String managerToClientsURL;


    public static void main(String[] args) throws FileNotFoundException {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate, optional)]
        // String inputFileName = args[0];
        //  String outputFileName = args[1];
        //  String tasksPerWorker = args[2];
        // boolean terminate = args.length > 3 && args[3].equals("terminate");
        boolean terminate = args[args.length - 1].equals("terminate");
        String tasksPerWorker = terminate ? args[args.length - 2] : args[args.length - 1];
        int numOfInputFiles = argsCheck(args, terminate);
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


        uploadInputstoS3(args, numOfInputFiles, tasksPerWorker);

        // loop for waiting for results
        int numOfOutputs = 0;
        boolean isfinished = false;
        List<String> argsList=Arrays.asList(args);
        while (!isfinished) {
            List<Message> messages = aws.receiveMessage(managerToClientsURL, 5);
            for (Message m : messages) {
                {
                   InputStream content = aws.getFile(bucketName, m.body());
                   int inputIndexInArgs=argsList.indexOf(m.body().split("\t")[2]);
                   createHtml(content,args[inputIndexInArgs+numOfInputFiles]);
                   numOfOutputs++;
                }

            }
            if (numOfOutputs == numOfInputFiles) {
                isfinished = true;
            }


        }


    }

    //Create Buckets, Create Queues
    private static void setup() {
        System.out.println("[DEBUG] Create bucket if not exist.");
        aws.createBucketIfNotExists("inputobjects");
        activeManagerIfNotActive();
        //processRequest(inputFileName,tasksPerWorker);

    }

    //checks if the number of input file equals to the number of output file
    //and return the number of inputfiles. if its not equal, return -1;
    public static int argsCheck(String[] args, boolean terminate) {
        if (terminate) {
            if (args.length % 2 == 0) {
                return (args.length - 2) / 2;
            } else {
                return -1;
            }
        } else {
            if (args.length % 2 == 1) {
                return (args.length - 1) / 2;
            } else {
                return -1;
            }
        }
    }



    private static void activeManagerIfNotActive() {
        managerId = aws.checkIfManagerExist();
        if (managerId == null) {
            System.out.println("Creating SQS...");
            clientsToManagerURL = aws.createSQS(sqsOut);
            managerToClientsURL = aws.createSQS(sqsIn);
            System.out.println("Finished creating SQS");

            managerId = aws.createEC2Manager();
            System.out.println("Manager activated");
        } else {
            clientsToManagerURL = aws.getQueueUrl(sqsOut);
            managerToClientsURL = aws.getQueueUrl(sqsIn);
            System.out.println("Manager is already active");
        }
    }




    private static void uploadInputstoS3(String[] args, int numOfInputFiles, String tasksPerWorker) throws FileNotFoundException {
        if (numOfInputFiles != -1) {
            for (int i = 0; i < numOfInputFiles; i++) {

                uploadSingleInputToS3(args[i],tasksPerWorker);

            }
        } else {
            System.out.println("wrong args");
        }
    }

    private static void uploadSingleInputToS3(String inputPath, String tasksPerWorker) throws FileNotFoundException {

        List<String> batchesToUpload =jsonparser(inputPath);
        int i=0;
        int totalreviews=0;
        for (String batch: batchesToUpload) {
                i++;

                String [] s=batch.split("\t",2);
                totalreviews+= Integer.parseInt(s[0]);
                aws.uploadString(bucketName, "input"+"\t"+clientId + "\t" + inputPath+"\t"+i , batch);
            }

        String message = "input"+"\t"+clientId + "\t" + inputPath+"\t"+batchesToUpload.size()+"\t"+totalreviews;
        aws.sendMessage(message, clientsToManagerURL);

    }

    public static void createHtml(InputStream inputStream, String outputName) {
        try {
            FileWriter htmlFile = new FileWriter(outputName + ".html");
            htmlFile.write("<html>\n");
            htmlFile.write("<head>\n");
            htmlFile.write("</head>\n");
            htmlFile.write("<body>\n");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    htmlFile.write(line+"\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            htmlFile.write("</body>\n");
            htmlFile.write("</html>\n");

            htmlFile.close();

        } catch (IOException e) {
            System.out.println("An error occurred while creating the HTML file.");
            e.printStackTrace();
        }

    }


    public static List<String> jsonparser(String path) throws FileNotFoundException {

        List<String> reviewsList = new ArrayList<String>();

        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line;
            int reviewcount=0;
            while ((line = reader.readLine()) != null) {
                // Create a JSONTokener with the JSON string
                JSONTokener tokener = new JSONTokener(line); // Create a JSONObject using the JSONTokener
                JSONObject jsonObject = new JSONObject(tokener);
                StringBuilder reviewsString= new StringBuilder();

                // Access the values of the keys
                JSONArray reviewsArray = jsonObject.getJSONArray("reviews");
                for(int i=0;i<reviewsArray.length();i++) {
                    reviewcount++;
                    JSONObject reviewObject = reviewsArray.getJSONObject(i);
                    reviewsString.append(reviewObject.getString("link")).append("\t").append(reviewObject.getString("text")).append("\t").append(reviewObject.getInt("rating")).append("\n");
                }

                reviewsString.insert(0, reviewsArray.length()+"\t");
                reviewsList.add(reviewsString.toString());
            }


            return reviewsList;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }



}





