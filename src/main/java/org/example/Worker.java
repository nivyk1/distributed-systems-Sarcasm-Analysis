package org.example;

import java.util.List;

public class Worker {
    private static sentimentAnalysisHandler sentimenthandler=new sentimentAnalysisHandler();
   private static namedEntityRecognitionHandler entityhandler=new namedEntityRecognitionHandler();

    String[] colors = {
            "dark red",
            "red",
            "black",
            "light green",
            "dark green"
    };

    public Worker() {

    }


    public String jobproccess(String job)
    {
        String [] reviews= job.split("\n");
        String result="";
        for(int i=0;i< reviews.length;i++) {
        String [] singleReview=reviews[i].split("niv");
            result+=ReviewHtmlmaker(singleReview);

        }

       return result;

    }


    // get the review and return a single line of the requested  output format for this specific review.
    public String ReviewHtmlmaker(String [] review) {
        String result = "";
        int sentiment = sentimenthandler.findSentiment(review[1]);
        result = "<p><a href=\"" + review[0] + "\" " + "style=\"color: " + colors[sentiment] + ";\">link</a>\t[";
        List<String> reviewEntities = entityhandler.findEntities(review[1]);
        if (!reviewEntities.isEmpty()) {
            for (String s : reviewEntities) {
                result += s + ",";
            }
            result = result.substring(0, result.length() - 1);
        }

        result+="]";

        if (Integer.parseInt(review[2]) == sentiment) {
            result += "\t no sarcasm</p>\n";
        } else {
            result += "\t sarcasm</p>\n";
        }


        return result;

    }




}
