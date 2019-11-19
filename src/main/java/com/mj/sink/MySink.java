package com.mj.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;


public class MySink extends AbstractSink implements Configurable {
    private String prefix;
    private String subfix;
    private String requestUrl = "http://localhost:8080/commen";
    private static Logger logger= LoggerFactory.getLogger(MySink.class);
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        HttpURLConnection conn = null;
        InputStream is = null;
        BufferedReader br = null;
        StringBuilder result = new StringBuilder();
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event event = channel.take();
            // 处理事件
            if (event != null) {
                String body = new String(event.getBody());
                System.out.println(body);
                requestUrl = requestUrl + "?test=" + body;
                System.out.println(requestUrl);
                //创建远程url连接对象
                URL url = new URL(requestUrl);
                //通过远程url连接对象打开一个连接，强转成HTTPURLConnection类
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                //发送POST请求必须设置为true
                conn.setDoOutput(true);
                conn.setDoInput(true);
                //设置连接超时时间和读取超时时间
                conn.setConnectTimeout(30000);
                conn.setReadTimeout(10000);
                conn.setRequestProperty("Content-Type", "application/json");
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
                        System.out.println(line);
                    }
                }else{
                    System.out.println("ResponseCode is an error code:" + conn.getResponseCode());
                }
                transaction.commit();
            }else{
                logger.info("even 为空！");
            }
            status = Status.READY;
        } catch (Exception e) {
            e.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
        } finally {
            if (transaction != null) {
                transaction.close();
            }
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
        return status;
    }

    @Override
    public void configure(Context context) {
        context.getString("subfix");
        context.getString("prefix");
    }
}
