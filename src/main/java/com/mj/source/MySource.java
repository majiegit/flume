package com.mj.source;

import cn.hutool.json.JSONUtil;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;


public class MySource extends AbstractSource implements Configurable, PollableSource{

    private static Logger logger= LoggerFactory.getLogger(MySource.class);

    private String requestUrl="http://39.107.49.58:8080/user";
    @Override
    public Status process() throws EventDeliveryException {
        HttpURLConnection conn = null;
        InputStream is = null;
        BufferedReader br = null;
        StringBuilder result = new StringBuilder();
        try{
            //创建远程url连接对象
            URL url = new URL(requestUrl);
            //通过远程url连接对象打开一个连接，强转成HTTPURLConnection类
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            //设置连接超时时间和读取超时时间
            conn.setConnectTimeout(15000);
            conn.setReadTimeout(60000);
            conn.setRequestProperty("Accept", "application/json");
            //发送请求
            conn.connect();
            //通过conn取得输入流，并使用Reader读取
            if (200 == conn.getResponseCode()){
                is = conn.getInputStream();
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                String line;
                while ((line = br.readLine()) != null){
                    result.append(line);
                }
            }else{
                System.out.println("ResponseCode is an error code:" + conn.getResponseCode());
            }
        }catch (MalformedURLException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                if(br != null){
                    br.close();
                }
                if(is != null){
                    is.close();
                }
            }catch (IOException ioe){
                ioe.printStackTrace();
            }
            conn.disconnect();
        }
        String s = result.toString();
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("status","200");
        map.put("String",result);

        SimpleEvent event = new SimpleEvent();
        // 将事件传给channel
        event.setBody(JSONUtil.toJsonStr(map).getBytes());

        getChannelProcessor().processEvent(event);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Status.READY;
    }
    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }
    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
    @Override
    public void configure(Context context) {

    }
}
