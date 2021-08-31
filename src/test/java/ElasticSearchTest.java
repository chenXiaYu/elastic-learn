import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;

public class ElasticSearchTest {
    @Test
    public void ²éÑ¯Êý¾Ý() throws IOException {
        RestHighLevelClient client =
                new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost("127.0.0.1",9200,"http")));
        GetRequest getRequest = new GetRequest("book","1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        if(getResponse.isExists()){
            long version = getResponse.getVersion();
            String data = getResponse.getSourceAsString();
            System.out.println(data);
        }

    }
}
