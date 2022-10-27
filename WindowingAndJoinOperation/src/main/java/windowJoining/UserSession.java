package windowJoining;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.io.Serializable;

public class UserSession implements Serializable {

    public static final String[] OUTPUT_FILE_HEADER_MAPPING = {
            "sessionId","userId","movieId", "movieName", "tag","timestamp"
    };

    private Integer sessionId;
    private String userId;
    private String movieId;
    private String tag;
    private Instant timestamp;

    private String movieTitle;

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

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getMovieTitle() {
        return movieTitle;
    }

    public void setMovieTitle(String movieTitle) {
        this.movieTitle = movieTitle;
    }

    public String asCSVRow(String delimiter) {
        return this.sessionId + delimiter + " "
             + this.userId + delimiter + " "
             + this.movieId + delimiter + " "
             + this.movieTitle + delimiter + " "
             + this.tag + delimiter + " "
             + this.timestamp;
    }

    public static String getCSVHeaders() {
        StringBuilder s = new StringBuilder();
        for (String column: OUTPUT_FILE_HEADER_MAPPING){
            s.append(column).append(", ");
        }
        return s.substring(0, s.length() - 2);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UserSession userSession = (UserSession) o;

        return  sessionId.equals(userSession.sessionId) &&
                userId.equals(userSession.userId) &&
                movieId.equals(userSession.movieId) &&
                tag.equals(userSession.tag) &&
                timestamp.equals(userSession.timestamp);
    }
}