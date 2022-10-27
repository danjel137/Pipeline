package window;

import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Objects;

public class MovieTag implements Serializable {
    public static String[] header_maping={"sessionId","userId","movieId","tag","timestamp"};
    //public static String arr[]=header.split(",");

//    public static StringBuilder getCsvHeader(){
//        StringBuilder s=new StringBuilder();
//        for(String colum:arr){
//            s.append(colum).append(", ");
//        }
//        System.out.println(s.substring(0,s.length()-2));
//        return s;
//    }




    private Integer sessionId;
    private String userId;
    private String movieId;
    private String tag;
    private static Instant timestamp;


    public Integer getSessionId() {
        return sessionId;
    }

    public void setSessionId(Integer sessionId) {
        this.sessionId = sessionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public   Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }


    public String asCsvRow(String delimiter) {
        return this.sessionId +delimiter+" "
                +this.userId+delimiter+" "
                +this.movieId+delimiter+" "
                +this.tag+delimiter+" "
                +this.timestamp;
    }

    public static String getCSVHeader(){
        StringBuilder s=new StringBuilder();
        for(String colum:header_maping){
            s.append(colum).append(", ");
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieTag movieTag = (MovieTag) o;
        return Objects.equals(sessionId, movieTag.sessionId) && Objects.equals(userId, movieTag.userId) && Objects.equals(movieId, movieTag.movieId) && Objects.equals(tag, movieTag.tag) && Objects.equals(timestamp, movieTag.timestamp);
    }



}
