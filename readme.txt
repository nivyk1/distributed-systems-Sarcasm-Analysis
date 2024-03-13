Nitay Maoz 313359341

Niv Yaakov 315311118



The app receives as an input text files that each one is a json that represents Reviews on amazon.
The app connects to Aws services, using S3 buckets, simple queue service, and EC2 instances.
The instances that are used for both manager and workers are T3.Large-type computers using the "ami-00e95a9222311e8ed" AMI.
the Local app will use a 'Manager' ec2 instance that will receive requests from multiple users at the same time, and launch ec2 'workers' instances that will perform the required tasks-Sentiment analysis & Named entity recognition, after which they will upload the result to the S3 and send a message to the Manager Node through a unique SQS for that input ,when all batches of the same file are received in the manager, it will create a summary file and send it back to the user.


 running the system with an 5 input files while choosing the number of workers per task  to be 70, took approximately 27 minutes[1638 seconds to be exact ].


To Run the application, follow the next instructions:
1)Create a S3 bucket named: nitay-aws-jar
** if the name of the bucket is occupied, you can give it a different name but you'll need to change the value of the appropriate field("Jars_Bucket_name") in class AWS.
2)To compile the application, the user will have to create 2 JAR files named "worker" & "manager" each with their own main class,you can create the jars using the IDEA or maven, we used the IDEA. To Choose different main class go to "file" --> "Project Structure" --> "+"-->"Jar" --> "From moudle with dependencies" and then choose your main class. Now we will need to build the jars, first go to the Manifest file and change the path to the correct main class, afterwards click on Build --> Build Artifacts and choose the class you wanted.
3)Upload the jar files to the S3 bucket you opened called "nitay-aws-jar" make sure the jars are named worker.jar & manager.jar
5)To set the input files, state the full path on the environment variables, after writing all the paths of the input files add to the environment variables output fies names for each input, afterwards add a number "tasksperworker" of your choosing and it's optional to add "terminate" command afterwards if you want to program to shutdown once it's finished
6)Run the "App" class from your computer and onces it's finished the html files will be placed on your project folder.



How does it work:


when running the application, the LocalApp is:
1)Generates a unique id for you.[we'll call it ClientID]
   This id will help to differentiate between different files in the bucket, associated with different people running the application at the same time and different files stored in s3 buckets.
2)checks if a Manager instance exists, creates one if not, Create 2 sqs [to and from the client to the manager].
3) parse the json  and uploads the information to the s3 with key made with the format "input"+clientID"+filename"+batchnumber"[as we were allowed by amazon to run only 9 instances we decided to give the parsing job to the local app but in reality there should be workers their tasks are to do the parsing].
5) send a message to the manager with the requested job and its details.
6)wait for result and finallly create html file.



After being created, the manager is:
1)setup - Create an SQS [to the workers from the manager], get the previous URL's of the SQS the LocalApp opened and get the managerID from ec2.
2) get into its main loop: Recieve messages from client, check if it's a "terminate message" if not, the main thread will create EC2 workers to work on the input and then create & run a new thread that does the following[a thread for each input file]:

1. Open an sqs for receiving the messages back from the worker.
2. Send the tasks to the workers, through 1  SQS that the manager created in the setup (all the tasks sent to the workers are sent to the same  SQS-ManagerToWorkers).
3. wait for all the messages indicating that the workers finished working on the file.
4. concatenating all the results, uploads to S3 the final result,send a message to the local App that the job was finished.
5. Thread finished it job and closes.


3)upon termination message ,stop receiving more messages from clients and wait for all threads to finish their work then close all the workers,queues and s3 bucket and self terminate.

Worker:
gets a message from an sqs queue , process the message  uploads to s3 the result and send a message to the sqs that the relevant thread is listening to, afterwards it deletes the message it worked on from the queue.

Security: 
Every Bucket and SQS can be created as private, meaning you can access it only using the credentials given to you by AWS.
the code does not contain any reference to the access tokens. it is hidden inside the credentials thus making the code safe to use and share.

Scalability: 
in this student environment, we could activate no more than 9 instances at the time.
if we ignore this limitation and could run as many instances as we need, our implementation could be very scalable.
the local app is currently doing the parsing job,parse every input and uploading each row to the S3 but in reality there should be workers that their task is to do the parsing, because of the student AWS limitations of 9 instances we decided it will be more scalable and work faster this way.
the workers are doing all the hard work from different instances.  they don't need a lot of memory since they are not keeping earlier tasks or results.
the manager is implemented in a thread-per-input manner.
its main thread is responsible to pull requests and initiating the worker instances, and every other thread is responsible for creating the tasks.
for a big number of user requests, we can easily allow defining more than one manager (the assignment requirement allowed us only one) making every part of this app scalable.

Persistence:
the way we implemented the application every task is deleted from the queue if and only if the task was processed and a result message was sent successfully.
if a node died in the middle of processing a certain task, the message will return to be visible after the visibility time ends[different for every queue], allowing other node to pull the message and proccess it.
by this logic, every task will be taken care of properly.

Tests:
this application was tested using input files of different sizes and handling different scenarios such as more than one request happening simultaneously, different combinations of requests with and without terminated messages.