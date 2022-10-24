package demo4;

import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;

public class SalesRecord1 {
    public final Long id;

    public final String dateTime;
    public final String product;
    public final float price;
    public final String paymentType;
    public final String country;

    public SalesRecord1( String dateTime, String product, float price, String paymentType, String country) {
        this.id = Math.round(Math.random());
        this.dateTime = dateTime;
        this.product = product;
        this.price = price;
        this.paymentType = paymentType;
        this.country = country;
    }

    @Override
    public String toString() {
        return "SalesRecord1{" +
                "id=" + id +
                ", dateTime='" + dateTime + '\'' +
                ", product='" + product + '\'' +
                ", price=" + price +
                ", paymentType='" + paymentType + '\'' +
                ", country='" + country + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SalesRecord1 record = (SalesRecord1) o;

        return dateTime.equals(record.dateTime) &&
                product.equals(record.product) &&
                price == record.price &&
                paymentType.equals(record.paymentType) &&
                country.equals(record.country);
    }
}
