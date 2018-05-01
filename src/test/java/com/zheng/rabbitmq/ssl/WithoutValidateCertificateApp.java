package com.zheng.rabbitmq.ssl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.zheng.rabbitmq.Constants;

/**
 * @Author zhenglian
 * @Date 2018/5/1 13:57
 */
public class WithoutValidateCertificateApp {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.SSL_PORT);

        factory.useSslProtocol();
        // Tells the library to setup the default Key and Trust managers for you
        // which do not do any form of remote server trust verification

        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        //non-durable, exclusive, auto-delete queue
        channel.queueDeclare("rabbitmq-java-test", false, true, true, null);
        channel.basicPublish("", "rabbitmq-java-test", null, "Hello, World".getBytes());


        GetResponse chResponse = channel.basicGet("rabbitmq-java-test", false);
        if (chResponse == null) {
            System.out.println("No message retrieved");
        } else {
            byte[] body = chResponse.getBody();
            System.out.println("Recieved: " + new String(body));
        }


        channel.close();
        conn.close();
    }
}
