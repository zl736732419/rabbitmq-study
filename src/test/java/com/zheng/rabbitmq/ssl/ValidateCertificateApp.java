package com.zheng.rabbitmq.ssl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.zheng.rabbitmq.Constants;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;

/**
 * @Author zhenglian
 * @Date 2018/5/1 14:20
 */
public class ValidateCertificateApp {
    public static void main(String[] args) throws Exception {
        char[] keyPassphrase = "MySecretPassword".toCharArray();
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(new FileInputStream("C:\\Users\\zhenglian\\Desktop\\keycert.p12"), keyPassphrase);

        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, keyPassphrase);

        char[] trustPassphrase = "zheng123".toCharArray();
        KeyStore tks = KeyStore.getInstance("JKS");
        tks.load(new FileInputStream("C:\\Users\\zhenglian\\Desktop\\rabbitstore"), trustPassphrase);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(tks);

        SSLContext c = SSLContext.getInstance("TLSv1.2");
        c.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.HOST);
        factory.setPort(Constants.SSL_PORT);
        factory.useSslProtocol(c);

        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        channel.queueDeclare("rabbitmq-java-test", false, true, true, null);
        channel.basicPublish("", "rabbitmq-java-test", null, "Hello, World".getBytes());


        GetResponse chResponse = channel.basicGet("rabbitmq-java-test", false);
        if(chResponse == null) {
            System.out.println("No message retrieved");
        } else {
            byte[] body = chResponse.getBody();
            System.out.println("Recieved: " + new String(body));
        }

        channel.close();
        conn.close();
    }
}
