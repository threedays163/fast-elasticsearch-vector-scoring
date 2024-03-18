package com.liorkn.elasticsearch;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Lior Knaany on 4/7/18.
 */
public class PluginTest {

    private static IndicesAdminClient indicesAdminClient;

    private static TransportClient client;

    private static RestHighLevelClient restHighLevelClient;

    private static String indexName = "knn_test";

    @BeforeClass
    public static void init() throws Exception {

        Settings settings = Settings.builder()
                .put("client.transport.sniff", "false")
                .put("cluster.name", "es-clusters")
                .build();
        client = new PreBuiltTransportClient(settings);
        String[] hostArray = new String[]{"127.0.0.1"};

        Arrays.stream(hostArray).forEach(host -> {
            try {
                InetAddress inetAddress = InetAddress.getByName(host);
                TransportAddress transportAddress = new TransportAddress(inetAddress, 9300);
                client.addTransportAddress(transportAddress);
            } catch (Exception e) {
            }
        });
        indicesAdminClient = client.admin().indices();

        HttpHost[] httpHosts = Arrays.stream(hostArray)//
                .map(host -> new HttpHost(host, 9200, "http")).toArray(HttpHost[]::new);

        RestClientBuilder builder = RestClient.builder(httpHosts);

        restHighLevelClient = new RestHighLevelClient(builder);

    }

    private static void rebuildIndex() {
        // delete test index if exists
        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            indicesAdminClient.delete(deleteIndexRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // create test index
        String mappingJson = "{\n" +
                "  \t\"doc\":{\n" +
                "\t  \t\"properties\": {\n" +
                "\t      \"embedding_vector\": { \"doc_values\": true,\"store\":true,\"type\":\"binary\" },\n" +
                "\t      \"job_id\":{\"type\":\"long\"},\n" +
                "\t      \"vector\":{\"type\":\"float\"}\n" +
                "\t    }\n" +
                "  \t}\n" +
                "}";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.mapping("doc", mappingJson, XContentType.JSON);
        CreateIndexResponse response = null;

        try {
            response = indicesAdminClient.create(createIndexRequest).actionGet();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    @Test
    public void search() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(10);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types("doc");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = null;
        try {
            response = restHighLevelClient.search(searchRequest);
        } catch (Exception e) {
            response = null;
            e.printStackTrace();
        }
        for (SearchHit hit : response.getHits().getHits()) {
            Object obj = hit.getSourceAsMap().get("vector");
            System.out.println(obj.getClass());
        }
        System.out.println(response);
    }

    @Test
    public void queryCos() {
        // 创建评分函数的脚本
        Map<String, Object> params = new HashMap<>();
        params.put("cosine", true);
        params.put("field", "embedding_vector");
        params.put("vector", new double[]{0.1, 0.2, 0.3});

        Script script = new Script(
                Script.DEFAULT_SCRIPT_TYPE,
                "knn",
                "binary_vector_score",
                params
        );

        // 创建评分函数
        ScoreFunctionBuilder scoreFunction = new ScriptScoreFunctionBuilder(script);

        // 创建查询和评分函数的组合
        FunctionScoreQueryBuilder query = new FunctionScoreQueryBuilder(scoreFunction)
                .boostMode(CombineFunction.REPLACE);

        System.out.println(query.toString());

        // 执行查询
        SearchResponse response = client.prepareSearch(indexName)
                .setQuery(query)
                .setSize(100)
                .get();
        // Test cosine score function
//        String body = "{" +
//                "  \"query\": {" +
//                "    \"function_score\": {" +
//                "      \"boost_mode\": \"replace\"," +
//                "      \"script_score\": {" +
//                "        \"script\": {" +
//                "          \"source\": \"binary_vector_score\"," +
//                "          \"lang\": \"knn\"," +
//                "          \"params\": {" +
//                "            \"cosine\": true," +
//                "            \"field\": \"embedding_vector\"," +
//                "            \"vector\": [" +
//                "               0.1, 0.2, 0.3" +
//                "             ]" +
//                "          }" +
//                "        }" +
//                "      }" +
//                "    }" +
//                "  }," +
//                "  \"size\": 100" +
//                "}";

        response.getHits().forEach(hit -> {
            System.out.println(hit.getScore());
        });
    }

    @Test
    public void queryDot() {
        // Test dot-product score function
        Map<String, Object> params1 = new HashMap<>();
        params1.put("cosine", false);
        params1.put("field", "embedding_vector");
        params1.put("vector", new double[]{0.1, 0.2, 0.3});

        Script script1 = new Script(
                Script.DEFAULT_SCRIPT_TYPE,
                "knn",
                "binary_vector_score",
                params1
        );

        // 创建评分函数
        ScoreFunctionBuilder scoreFunction1 = new ScriptScoreFunctionBuilder(script1);

        // 创建查询和评分函数的组合
        FunctionScoreQueryBuilder query1 = new FunctionScoreQueryBuilder(scoreFunction1)
                .boostMode(CombineFunction.REPLACE);

        // 执行查询
        SearchResponse response2 = client.prepareSearch(indexName)
                .setQuery(query1)
                .setSize(100)
                .get();

        System.out.println(response2);
        response2.getHits().forEach(hit -> {
            System.out.println(hit.getScore());
        });

//        body = "{" +
//                "  \"query\": {" +
//                "    \"function_score\": {" +
//                "      \"boost_mode\": \"replace\"," +
//                "      \"script_score\": {" +
//                "        \"script\": {" +
//                "          \"source\": \"binary_vector_score\"," +
//                "          \"lang\": \"knn\"," +
//                "          \"params\": {" +
//                "            \"cosine\": false," +
//                "            \"field\": \"embedding_vector\"," +
//                "            \"vector\": [" +
//                "               0.1, 0.2, 0.3" +
//                "             ]" +
//                "          }" +
//                "        }" +
//                "      }" +
//                "    }" +
//                "  }," +
//                "  \"size\": 100" +
//                "}";
//        searchRequest.setJsonEntity(body);
//        res = esClient.performRequest(searchRequest);
//        System.out.println(res);
//        resBody = EntityUtils.toString(res.getEntity());
//        System.out.println(resBody);
//        Assert.assertEquals("search should return status code 200", 200, res.getStatusLine().getStatusCode());
//        Assert.assertTrue(String.format("There should be %d documents in the search response", objs.length), resBody.contains("\"hits\":{\"total\":" + objs.length));
//        // Testing Scores
//        hitsJson = (ArrayNode) mapper.readTree(resBody).get("hits").get("hits");
//        Assert.assertEquals(1.5480561, hitsJson.get(0).get("_score").asDouble(), 0);
//        Assert.assertEquals(1.4918247, hitsJson.get(1).get("_score").asDouble(), 0);
    }

    @Test
    public void insert() throws Exception {
        rebuildIndex();

        final ObjectMapper mapper = new ObjectMapper();
        final TestObject[] objs = {new TestObject(1, new float[]{0.0f, 0.5f, 1.0f}),
                new TestObject(2, new float[]{0.2f, 0.6f, 0.99f})};

        for (int i = 0; i < objs.length; i++) {
            final TestObject t = objs[i];
            final String json = mapper.writeValueAsString(t);
            System.out.println(json);
            restHighLevelClient.index(new IndexRequest(indexName, "doc").id(String.valueOf(t.jobId)).source(json, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
        }


        // Testing Scores
//        ArrayNode hitsJson = (ArrayNode) mapper.readTree(resBody).get("hits").get("hits");
//        Assert.assertEquals(0.9970867, hitsJson.get(0).get("_score").asDouble(), 0);
//        Assert.assertEquals(0.9780914, hitsJson.get(1).get("_score").asDouble(), 0);


    }

    @AfterClass
    public static void shutdown() {
//        try {
//            esClient.close();
//            esServer.shutdown();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

}
