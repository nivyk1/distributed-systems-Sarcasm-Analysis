package org.example;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


import java.io.File;
import java.io.InputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

public class AWS {
    private final String managerScript = "#! /bin/bash\n" +
            "AWS_ACCESS_KEY_ID="+System.getenv("AWS_ACCESS_KEY_ID")+
            "AWS_SECRET_ACCESS_KEY="+ System.getenv("AWS_SECRET_ACCESS_KEY")+
            "AWS_SESSION_TOKEN=" +System.getenv("AWS_SESSION_TOKEN")+"\n"+
            "sudo yum update -y\n" +
            "sudo yum install -y java-21-amazon-corretto\n" +
            "mkdir ManagerFiles\n" +
            "aws s3 cp s3://" + AWS.Jars_Bucket_name + "/manager.jar ./ManagerFiles\n" +
            "java -jar /ManagerFiles/manager.jar\n";

    public final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static String ami = "ami-00e95a9222311e8ed"; //linux and java
    //ami-00e95a9222311e8ed
    //0ff8a91507f77f867
    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;
    public static final int MAX_WORKERS_INSTANCES = 8; // maximum instances for a student account (real max is 9 but one is for the manager)

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    public static final String jar_test = "test-jar";
    public static final String input_Output_Bucket = "super-duper-bucket-nitay";
    public static final String Output_Bucket_name = "";
    public static final String Jars_Bucket_name = "nitay-aws-jar";


    // S3
    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // EC2
    //Manager
    public String createEC2Manager() {
        Ec2Client ec2 = Ec2Client.builder().region(region2).build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T3_LARGE)
                .imageId(ami)
                .maxCount(1)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::905418107445:instance-profile/LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((managerScript).getBytes()))
                .build();


        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value("Manager")
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    public String createEC2(String script, String tagName, int numberOfInstances) {
        Ec2Client ec2 = Ec2Client.builder().region(region2).build();
        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceType(InstanceType.T3_LARGE)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();


        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    public void createSqsQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(Collections.singletonMap(QueueAttributeName.VISIBILITY_TIMEOUT, "600"))
                .build();
        sqs.createQueue(createQueueRequest);
    }
//    public void createSqsQueue(String queueName) {
//        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
//                .queueName(queueName)
//                .attributes(Collections.singletonMap(QueueAttributeName.VISIBILITY_TIMEOUT.toString(), "90"))
//                .build();
//        sqs.createQueue(createQueueRequest);
//    }


    //this function checks all running instance for the manager tag
    //returns manager instance id if it exists, else returns null
    public String getManagerId() {
        String managerTag = "Manager";
        Filter filter = Filter.builder().name("instance-state-name").values("running").build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        for (Reservation res : response.reservations()) {
            for (Instance ins : res.instances()) {
                for (Tag tag : ins.tags()) {
                    if (tag.key().equals("Name") && tag.value().equals(managerTag)) {
                        return ins.instanceId();
                    }
                }
            }
        }
        return null;
    }
    //key is the clientID
    public void uploadFile(String bucketName, String key, File file) {
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .acl(ObjectCannedACL.PUBLIC_READ)
                        .build(),
                RequestBody.fromFile(file));
    }

    public void uploadString(String bucketName, String key, String content) {
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build(),
                RequestBody.fromString(content));
    }


    //SQS

    public void sendMessage(String message, String queueUrl) {
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(10)
                .build());
    }

    public String getQueueUrl(String queueName) {
        GetQueueUrlResponse getQueueUrlResponse =
                sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
        return getQueueUrlResponse.queueUrl();
    }

    public String createSQS(String name) {

        try {
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(name)
                    .build();

            sqs.createQueue(createQueueRequest);

            GetQueueUrlResponse getQueueUrlResponse = sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(name).build());
            return getQueueUrlResponse.queueUrl();

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "failed";
    }

    //pull messages from queue up to maximum of numOfMessages
    public List<Message> receiveMessage(String queueUrl, int numOfMessages) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .visibilityTimeout(600)
                .maxNumberOfMessages(numOfMessages)
                .build();
        return sqs.receiveMessage(receiveMessageRequest).messages();
    }
    public void deleteMessage(Message message, String queueUrl) {

        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
    }
    public InputStream getFile(String bucketName, String key){
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        return s3.getObject(getObjectRequest);
    }

    public int countWorkerInstances() {
        String workerTag = "worker";
        int count = 0;

        Filter filter = Filter.builder().name("instance-state-name").values("pending", "running").build();
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().filters(filter).build();
        DescribeInstancesResponse response = ec2.describeInstances(request);

        for (Reservation res : response.reservations()) {
            for (Instance ins : res.instances()) {
                for (Tag tag : ins.tags()) {
                    if (tag.key().equals("Name") && tag.value().equals(workerTag)) {
                        count++;
                        break; // Found a worker instance, move to the next instance
                    }
                }
            }
        }
        return count;
    }

    public void deleteAllQueues(String managerToClientsURL) {
        try {
            // Get the URL of the managerToClients queue
            // Wait until the managerToClients queue is empty
            while (!isQueueEmpty(managerToClientsURL)) {
                System.out.println("Waiting for managerToClients queue to become empty...");
                Thread.sleep(1000); // Wait for 1 second before checking again
            }

            // Get a list of all SQS queues
            ListQueuesResponse listQueuesResponse = sqs.listQueues();
            List<String> queueUrls = listQueuesResponse.queueUrls();

            // Iterate over each queue URL
            for (String queueUrl : queueUrls) {
                // Delete the queue
                DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                        .queueUrl(queueUrl)
                        .build();
                sqs.deleteQueue(deleteQueueRequest);
                System.out.println("Deleted queue: " + queueUrl);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupted status
            System.err.println("Thread interrupted while waiting: " + e.getMessage());
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
    public void deleteSingleQueue(String queueUrl)
    {
        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                .queueUrl(queueUrl)
                .build();
        sqs.deleteQueue(deleteQueueRequest);
    }


    //this functions deletes all files from a bucket and then deleting the bucket, this works on all buckets except the Jar bucket
    public void deleteAllBuckets() {
        try {
            // Get a list of all S3 buckets
            ListBucketsResponse listBucketsResponse = s3.listBuckets();
            for (Bucket bucket : listBucketsResponse.buckets()) {
                String bucketName = bucket.name();
                if (!bucketName.equals(AWS.Jars_Bucket_name)) { // Delete everything except jar bucket
                    // Empty the bucket by deleting all objects (files) within it
                    ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                            .bucket(bucketName)
                            .build();
                    ListObjectsV2Response listObjectsResponse = s3.listObjectsV2(listObjectsRequest);
                    for (S3Object s3Object : listObjectsResponse.contents()) {
                        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                                .bucket(bucketName)
                                .key(s3Object.key())
                                .build();
                        s3.deleteObject(deleteObjectRequest);
                        //System.out.println("Deleted object: " + s3Object.key() + " from bucket: " + bucketName);
                    }
                    // Delete the bucket
                    DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                            .bucket(bucketName)
                            .build();
                    s3.deleteBucket(deleteBucketRequest);
                    System.out.println("Deleted bucket: " + bucketName);
                }
            }
        } catch (S3Exception e) {
            System.err.println("Error occurred: " + e.awsErrorDetails().errorMessage());
        }
    }
    public void terminateInstance(String instanceId) {
        try{
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            TerminateInstancesResponse response = ec2.terminateInstances(ti);
            System.out.println("Termination status for instance " + instanceId + ": " + response.terminatingInstances().get(0).instanceId());

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private boolean isQueueEmpty(String queueUrl) {
        try {
            // Get the attributes of the queue
            GetQueueAttributesResponse queueAttributesResponse = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                    .build());

            // Retrieve the approximate number of messages in the queue
            String numberOfMessagesStr = queueAttributesResponse.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);

            // Check if the attribute value is null
            if (numberOfMessagesStr == null) {
                System.err.println("Approximate number of messages attribute is null.");
                return false;
            }

            // Parse the number of messages into an integer
            int numberOfMessages = Integer.parseInt(numberOfMessagesStr);

            // If the number of messages is 0, the queue is considered empty
            return numberOfMessages == 0;
        } catch (SqsException e) {
            System.err.println("Error occurred while checking if the queue is empty: " + e.awsErrorDetails().errorMessage());
            return false; // Return false in case of any error
        } catch (NumberFormatException e) {
            System.err.println("Error occurred while parsing number of messages: " + e.getMessage());
            return false; // Return false if parsing fails
        }
    }

}
