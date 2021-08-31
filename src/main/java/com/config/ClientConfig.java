package com.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClientConfig {

    @Value("${elastic.host}")
    private String host;
    @Value("${elastic.requestType}")
    private String requestType;
    @Value("${elastic.port}")
    private Integer port;

    @Bean
    public RestHighLevelClient restHighLevelClient(){
        return  new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost(host,port,requestType)));
    }

}
