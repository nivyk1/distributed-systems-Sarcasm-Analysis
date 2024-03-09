package org.example;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Main {
    final static AWS aws = AWS.getInstance();
    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis()/1000;
        aws.createBucketIfNotExists(AWS.Jars_Bucket_name);
        System.out.println("created-jar-bucket");
    }




}


