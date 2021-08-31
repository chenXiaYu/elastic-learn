import com.MyStart;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest(classes = MyStart.class)
@RunWith(SpringRunner.class)
public class ElasticSearchSpringBootTest {

    @Autowired
    private RestHighLevelClient client;


    @Test
    public void ���Ը�������id��ѯ() throws IOException {
        GetRequest getRequest = new GetRequest("book","1");


        //�����ض��ֶ�
        String [] includes = new String[]{"name","description"};
        String [] excluedes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true,includes,excluedes);
        getRequest.fetchSourceContext(fetchSourceContext);

        //����ͬ������
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        if(getResponse.isExists()){
            long version = getResponse.getVersion();
            String data = getResponse.getSourceAsString();
            System.out.println(data);
            System.out.println("---------------------------------------------------------------------------------------");
        }

        ActionListener<GetResponse> getResponseActionListener = new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse response) {
                System.out.println("�첽�����������");
                System.out.println(response.getSourceAsString());
            }

            @Override
            public void onFailure(Exception e) {

            }
        };

        //�첽����
        client.getAsync(getRequest,RequestOptions.DEFAULT,getResponseActionListener);
    }

    /**
     * ��������
     *  PUT test_post/_doc/2
     *     {
     *         "user":"tomas",
     *             "postDate":"2019-07-18",
     *             "message":"trying out es1"
     *     }
     */
    @Test
    public void �������() throws IOException {
        IndexRequest request = new IndexRequest("test_posts");
        request.id("2");

        //json �ķ�ʽʵ��������
        String jsonString = "{\n" +
                "  \"user\":\"tomas\",\n" +
                "  \"postDate\":\"2019-07-18\",\n" +
                "  \"message\":\"trying out es1\"\n" +
                "}";
        request.source(jsonString, XContentType.JSON);


        request.source(jsonString, XContentType.JSON);

//        ��������2
//        Map<String,Object> jsonMap=new HashMap<>();
//        jsonMap.put("user", "tomas");
//        jsonMap.put("postDate", "2019-07-18");
//        jsonMap.put("message", "trying out es2");
//        request.source(jsonMap);

//        ��������3
//        XContentBuilder builder= XContentFactory.jsonBuilder();
//        builder.startObject();
//        {
//            builder.field("user", "tomas");
//            builder.timeField("postDate", new Date());
//            builder.field("message", "trying out es2");
//        }
//        builder.endObject();
//        request.source(builder);

//        ��������4
//        request.source("user","tomas",
//                    "postDate",new Date(),
//                "message","trying out es2");




        request.timeout(TimeValue.timeValueSeconds(10l));
        //request.timeout("1s);

        //�Լ�ά���汾
//        request.version(1);
//        request.versionType(VersionType.EXTERNAL);


        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        //Ҳ����ͨ�� indexAsync �����첽����

        String index = response.getIndex();
        String id = response.getId();
        System.out.println("index:"+index+" id:"+id);
        if(response.getResult() == DocWriteResponse.Result.CREATED){
            DocWriteResponse.Result result = response.getResult();
            System.out.println("CREATED: "+result);
        }else if( response.getResult() == DocWriteResponse.Result.UPDATED){
            DocWriteResponse.Result result = response.getResult();
            System.out.println("UPDATED: "+result);
        }

        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        if(shardInfo.getTotal() != shardInfo.getSuccessful()){
            System.out.println("����ɹ��ķ�Ƭ����������");
        }

        if(shardInfo.getFailed() > 0){
            for(ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()){
                System.out.println("��Ƭʧ��ԭ��"+failure.reason());
            }
        }
    }

    /**
     * post /test_posts/_doc/3/_update
     * {
     *    "doc": {
     *       "user"��"tomas J"
     *    }
     * }
     *
     */
    @Test
    public void �޸��ĵ�() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("test_posts","2");
        Map<String,Object> jsonMap = new HashMap<>();
        jsonMap.put("user","tomas J");
        updateRequest.doc(jsonMap);

        updateRequest.timeout("5s");
        //���Դ���
        updateRequest.retryOnConflict(3);
        //�ȴ��ɹ���Ƭ��Ŀ
//        updateRequest.waitForActiveShards(2);
        //�ȴ����еķ�Ƭ���ɹ�
//        updateRequest.waitForActiveShards(ActiveShardCount.ALL);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

        String id = updateResponse.getId();
        String index = updateResponse.getIndex();
        System.out.println("id:" + id + " index:"+index);

        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
            DocWriteResponse.Result result = updateResponse.getResult();
            System.out.println("CREATED:" + result);
        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            DocWriteResponse.Result result = updateResponse.getResult();
            System.out.println("UPDATED:" + result);
        }else if(updateResponse.getResult() == DocWriteResponse.Result.DELETED){
            DocWriteResponse.Result result = updateResponse.getResult();
            System.out.println("DELETED:" + result);
        }else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP){
            //û�в���
            DocWriteResponse.Result result = updateResponse.getResult();
            System.out.println("NOOP:" + result);
        }
    }


    /**
     * DELETE /test_posts/_doc/2
     */
    @Test
    public void ɾ���ĵ�() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("test_posts","2");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);

        System.out.println(deleteResponse.getResult());
    }


    /**
     * POST /_bulk
     * {"action": {"metadata"}}
     * {"data"}
     */
    @Test
    public void ������������() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("post").id("1").source(XContentType.JSON,"field","1"));
        bulkRequest.add(new IndexRequest("post").id("2").source(XContentType.JSON,"field","2"));
        //һ����������������

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        for(BulkItemResponse itemResponse : bulkResponse){
            DocWriteResponse itemResponseResponse = itemResponse.getResponse();

            switch (itemResponse.getOpType()) {
                case INDEX:
                case CREATE:
                    IndexResponse indexResponse = (IndexResponse) itemResponseResponse;
                    indexResponse.getId();
                    System.out.println(indexResponse.getResult());
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse) itemResponseResponse;
                    updateResponse.getIndex();
                    System.out.println(updateResponse.getResult());
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponseResponse;
                    System.out.println(deleteResponse.getResult());
                    break;
            }
        }
    }

    /**
     *GET /test_index/_mget
     * {
     *   "docs":[
     *     {"_id":5},
     *     {"_id":11}
     * ]
     * }
     * @throws IOException
     */
    @Test
    public void һ���Ի�ȡ����() throws IOException {
        MultiGetRequest multiGetRequest = new MultiGetRequest();

        MultiGetRequest.Item item5 = new MultiGetRequest.Item("test_index","5");
        MultiGetRequest.Item item11 = new MultiGetRequest.Item("test_index","11");
        multiGetRequest.add(item5);
        multiGetRequest.add(item11);

        MultiGetResponse mget = client.mget(multiGetRequest, RequestOptions.DEFAULT);
        for(MultiGetItemResponse response : mget){
            System.out.println(response.getResponse().getSourceAsString());
        }


    }
}
