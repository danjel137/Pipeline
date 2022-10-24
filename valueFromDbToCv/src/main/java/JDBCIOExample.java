import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JDBCIOExample {

   public static void main(String[] args) {
      Pipeline pipeline=Pipeline.create();

      PCollection<String>poutput= pipeline.apply(
              JdbcIO.<String>read().withDataSourceConfiguration(
              JdbcIO.DataSourceConfiguration
                      .create("org.postgresql.Driver","jdbc:postgresql://localhost:5432/postgres")
                      .withUsername("postgres")
                      .withPassword("12345600"))
              .withQuery("select name ,surname from peoplee where id= ? ")
              .withCoder(StringUtf8Coder.of())
              .withStatementPreparator(new JdbcIO.StatementPreparator() {
                 // @Override
                 public void setParameters(PreparedStatement preparedStatement) throws Exception {
                    preparedStatement.setInt(1,1);
                 }
              })
              .withRowMapper(new JdbcIO.RowMapper<String>() {
                 @Override
                 public String mapRow(ResultSet resultSet) throws Exception {
                    return resultSet.getString(1)+resultSet.getString(2);
                 }
              }));
      poutput.apply(TextIO.write().to("C:\\Users\\HP\\IdeaProjects\\Pipelines\\valueFromDbToCv\\src\\main\\resources\\values.csv").withNumShards(1).withSuffix(".csv"));
      pipeline.run();
   }

}
