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

    public void setPrice(Double price) {
        this.price = price;
    }
}
