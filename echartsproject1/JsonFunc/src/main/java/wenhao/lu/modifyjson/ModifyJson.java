package wenhao.lu.modifyjson;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class ModifyJson {
    public static void main(String[] args) throws IOException {
        JSONArray ja = new JSONArray();


        // 随机生成数据源
        Random random = new Random();
        //定义字段选取数据集
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav"};


        for(int i =0;i<10;i++){
            JSONObject jo = new JSONObject();
            String url = urls[random.nextInt(urls.length)];
            int num = random.nextInt(100);
            jo.put("url",url);
            jo.put("num",num);
            ja.put(jo);
        }


        File file = new File("D:\\code\\echart\\echartsproject1\\data\\json1.json");
        FileUtils.write(file, ja.toString(), "utf-8", false);

    }
}
