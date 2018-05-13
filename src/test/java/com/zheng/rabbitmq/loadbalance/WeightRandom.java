package com.zheng.rabbitmq.loadbalance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * 加权随机负载均衡算法
 * @Author zhenglian
 * @Date 2018/5/13 22:35
 */
public class WeightRandom {

    private List<Server> servers = new ArrayList<Server>();
    private int totalWeight; 

    public void init(){
        servers.add(new Server("192.168.1.101", 5));
        servers.add(new Server("192.168.1.102", 2));
        servers.add(new Server("192.168.1.103", 2));
        servers.add(new Server("192.168.1.104", 1));

        // 根据权重排序
        Collections.sort(servers, new Comparator<Server>() {
            @Override
            public int compare(Server o1, Server o2) {
                return o1.getWeight() - o2.getWeight();
            }
        });
        
        totalWeight = getTotalWeight();
    }

    /**
     * 获取总权重
     * @return
     */
    private int getTotalWeight() {
        int weight = 0;
        for(Server server : servers) {
            weight += server.getWeight();
        }
        return weight;
    }


    public Server getServer() {
        // 从[1, totalWeight]去一个数，遍历整个服务器集合，当遍历的节点权重和>=该随机值就取当前节点
        int randomWeight = new Random().nextInt(totalWeight) + 1;
        int sumWeight = 0;
        for (Server server : servers) {
            sumWeight += server.getWeight();
            if (sumWeight >= randomWeight) {
                return server;
            }
        }
        return null;
    }
    
    public static void main(String[] args) {
        WeightRandom weightRandom = new WeightRandom();
        weightRandom.init();
        Server server;
        for (int i = 0; i < weightRandom.totalWeight; i++) {
            server = weightRandom.getServer();
            System.out.println("server " + server.getIp() + " weight=" + server.getWeight());
        }
    }
    
    
}
