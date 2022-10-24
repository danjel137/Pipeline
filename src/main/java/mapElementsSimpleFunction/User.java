package mapElementsSimpleFunction;

import org.apache.beam.sdk.transforms.SimpleFunction;

public class User extends SimpleFunction<String, String> {
    @Override
    public String apply(String input) {
        String arr[] = input.split(",");
        String Session=arr[0];
        String Id=arr[1];
        String Name=arr[2];
        String Geneder=arr[3];
        String output="";
        if(Geneder.equals("1")){
            output=Session+","+Id+","+Name+","+"M";
        } else if (Geneder.equals("2")) {
            output=Session+","+Id+","+Name+","+"F";
        }else output=input;
        return output;
    }
}
