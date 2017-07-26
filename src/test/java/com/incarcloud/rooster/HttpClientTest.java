package com.incarcloud.rooster;/**
 * Created by fanbeibei on 2017/7/20.
 */


import com.incarcloud.rooster.util.HttpClientUtil;

/**
 * @author Fan Beibei
 * @Description: 描述
 * @date 2017/7/20 10:35
 */
public class HttpClientTest {
//    @Test
    public void testPostJson(){
        String url = "http://127.0.0.1:6666/rest";
        String data = "{\"cmdType\":\"OPEN_DOOR\",\"vin\":\"1A1JC5444R7252367\"}";


        try {
            String result = HttpClientUtil.postJson(url, data, "UTF-8", 3000, null);

            System.out.println(result);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}