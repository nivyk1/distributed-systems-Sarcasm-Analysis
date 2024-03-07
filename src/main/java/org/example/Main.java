package org.example;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static List<String> jsonparser(InputStream file) throws FileNotFoundException {

        List<String> reviewsList = new ArrayList<String>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file))) {
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

    public static void main(String[] args) {
        String filePath = "C:\\Users\\nitay\\Downloads\\input1.txt"; // File path


        List<String> reviewsList = null;
        try (InputStream inputStream = new FileInputStream(filePath)) {
            // Call your function with the inputStream
            reviewsList = jsonparser(inputStream);
        } catch (IOException e) {
            // Handle IO exceptions
            e.printStackTrace();
        }
        for (String review : reviewsList) {
            System.out.print(review);
        }
    }



}

