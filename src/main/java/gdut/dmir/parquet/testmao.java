package gdut.dmir.parquet;

import parquet.org.codehaus.jackson.JsonNode;
import parquet.org.codehaus.jackson.map.ObjectMapper;

/**
 * Created by Richard on 2016-07-30.
 */
public class testmao {
    public static void main(String[] args) {
        try{
            // 演示字符串
            String str = "{\"data\":{\"hasnext\":0,\"info\":[{\"id\":\"288206077664983\",\"timestamp\":1371052476},{\"id\":\"186983078111768\",\"timestamp\":1370944068},{\"id\":\"297031120529307\",\"timestamp\":1370751789},{\"id\":\"273831022294863\",\"timestamp\":1369994812}],\"timestamp\":1374562897,\"totalnum\":422},\"errcode\":0,\"msg\":\"ok\",\"ret\":0,\"seqid\":5903702688915195270}";

            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(str);

            // 提取 data
            JsonNode data = root.path("data");
            // 提取 info
            JsonNode info = data.path("info");

            System.out.println(info.size());

            // 得到 info 的第 0 个
            JsonNode item = info.get(0);
            System.out.println(item.get("id"));
            System.out.println(item.get("timestamp"));

            // 得到 info 的第 2 个
            item = info.get(2);
            System.out.println(item.get("id"));
            System.out.println(item.get("timestamp"));

            // 遍历 info 内的 array
            if (info.isArray())
            {
                for (JsonNode objNode : info)
                {
                    System.out.println(objNode);
                }
            }

        }
        catch (Exception e)
        {

        }
    }
}