import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONReader;

import java.io.*;
import java.lang.*;

public class JsonSplit {
    public static void main(String[] args) throws FileNotFoundException {
        readJson("book.json");
    }

    public static void writeToFile(String filePath, String tag){
        File file =new File(filePath);
        try{
            if(!file.exists()){
                file.createNewFile();
            }
            //使用true，即进行append file
            FileWriter fileWritter = new FileWriter(file.getPath(),true);
            fileWritter.write(tag);
            fileWritter.write(' ');
            fileWritter.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public static void readJson(String jsonPath) throws FileNotFoundException {
        int count = 0, path = 0;

        JSONReader reader = new JSONReader(new FileReader(jsonPath));
        reader.startObject();
        while(reader.hasNext()){
            while (reader.hasNext())
            {
                String key = reader.readString();
                if (key.equals("tags"))
                {
                    reader.startArray();
                    while (reader.hasNext())
                    {
                        String item = reader.readString();
                        if(count > 200){
                            path++;
                            count = 0;
                        }
                        writeToFile("tags/tags" + path + ".txt", item);
                        count++;
                        System.out.println(item);
                    }
                    reader.endArray();
                }
                else{
                    String value = reader.readString();
                }
            }
            reader.endObject();
            try{
                reader.startObject();
            }catch(JSONException jsonException){
                break;
            }
        }
        System.out.print("finished!");
    }
}
