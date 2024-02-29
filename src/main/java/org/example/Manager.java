package org.example;
import org.json.*;

import javax.json.JsonObject;
import javax.json.stream.JsonParser;
import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class Manager {


    public Manager() {
    }

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



