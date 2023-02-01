package GroupByKey;


import java.io.Serializable;

public class CarModel implements Serializable{
    private String model;
    private Double price;

    public CarModel(String model, Double price) {
        this.model = model;
        this.price = price;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public Double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "CarModel{" +
                "model='" + model + '\'' +
                ", price=" + price +
                '}';
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}
