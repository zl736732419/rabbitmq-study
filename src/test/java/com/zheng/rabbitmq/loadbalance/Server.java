package com.zheng.rabbitmq.loadbalance;

class Server{  
    private String ip;  
      
    private int weight;  
      
    public Server(String ip, int weight) {  
        this.ip = ip;  
        this.weight = weight;  
    }  
  
    public String getIp() {  
        return ip;  
    }  
  
    public void setIp(String ip) {  
        this.ip = ip;  
    }  
  
    public int getWeight() {  
        return weight;  
    }  
  
    public void setWeight(int weight) {  
        this.weight = weight;  
    }  
      
    @Override  
    public boolean equals(Object obj) {  
        if(this == obj) return true;  
          
        if(obj instanceof Server){  
            Server server = (Server)obj;  
          
            return getIp().equals(server.getIp());  
        }  
        return false;  
    }  
      
    @Override  
    public int hashCode() {  
        return getIp().hashCode();  
    }  
    
}  