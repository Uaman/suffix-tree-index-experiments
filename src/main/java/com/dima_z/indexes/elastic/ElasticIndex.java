package com.dima_z.indexes.elastic;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import com.dima_z.indexes.IIndex;
import com.dima_z.pojo.IndexValueObject;

public class ElasticIndex implements IIndex {

    private String ELASTIC_SERVER_URL = null;
    private String ELASTIC_USER = null;
    private String ELASTIC_HTTPS_FINGERPRINT = null;
    private String ELASTIC_PASSWORD = null;
    private String ELASTIC_INDEX_NAME = null;
    private RestClient restClient = null;
    private ElasticsearchClient esClient = null;
    private int numberOfItems = 0;

    private final String FIELD_NAME = "name";

    public ElasticIndex() {
        System.out.println("ElasticIndex created");
        this.readProperties();

        SSLContext sslContext = TransportUtils
            .sslContextFromCaFingerprint(ELASTIC_HTTPS_FINGERPRINT); 

        BasicCredentialsProvider credsProv = new BasicCredentialsProvider(); 
        credsProv.setCredentials(
            AuthScope.ANY, new UsernamePasswordCredentials(ELASTIC_USER, ELASTIC_PASSWORD)
        );

        this.restClient = RestClient
        .builder(HttpHost.create(ELASTIC_SERVER_URL)) 
        .setHttpClientConfigCallback(hc -> hc
            .setSSLContext(sslContext) 
            .setDefaultCredentialsProvider(credsProv)
        )
        .build();

        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        this.esClient = new ElasticsearchClient(transport);
    }

    private void readProperties() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            props.load(input);

            ELASTIC_SERVER_URL = props.getProperty("elasticServerUrl");
            ELASTIC_USER = props.getProperty("elasticUser");
            ELASTIC_PASSWORD = props.getProperty("elasticPassword");
            ELASTIC_HTTPS_FINGERPRINT = props.getProperty("elasticFingerprint");
            ELASTIC_INDEX_NAME = props.getProperty("elasticIndexName");

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void createIndex() { 
        try {

            FileReader file = new FileReader(new File("./src/main/resources/elastic-index.json"));

            CreateIndexRequest request = CreateIndexRequest.of(index -> index
                .index(ELASTIC_INDEX_NAME)
                .withJson(file)
            );

            System.out.println("createIndex " + this.esClient.ping().toString()); 
            this.esClient.indices().create(request);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dropIndex() {
        try {
            this.esClient.indices().delete(c -> c
                .index(ELASTIC_INDEX_NAME)
            );
            this.numberOfItems = 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isEmpty() {
        return this.numberOfItems == 0;
    }

    @Override
    public Collection<Integer> search(String query) {
        Collection<Integer> result = new ArrayList<Integer>();
        try {
            SearchResponse<IndexValueObject> response = esClient.search(s -> s
                .index(this.ELASTIC_INDEX_NAME) 
                .query(q -> q      
                    .match(t -> t   
                        .field(FIELD_NAME)  
                        .query(query)
                    )
                ),
                IndexValueObject.class      
            );

            result = processResponce(response);
        } catch (ElasticsearchException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> includes(String query) {
        Collection<Integer> result = new ArrayList<Integer>();
        try {
            SearchResponse<IndexValueObject> response = esClient.search(s -> s
                .index(this.ELASTIC_INDEX_NAME) 
                .query(q -> q    
                    .wildcard(t -> t   
                        .field(FIELD_NAME)  
                        .caseInsensitive(false)
                        .value("*" + query + "*")
                    )
                ),
                IndexValueObject.class      
            );

            result = processResponce(response);
        } catch (ElasticsearchException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> notIncludes(String query) {
        Collection<Integer> result = new ArrayList<Integer>();
        try {
            SearchResponse<IndexValueObject> response = esClient.search(s -> s
                .index(this.ELASTIC_INDEX_NAME) 
                .query(q -> q    
                    .bool(b -> b
                        .mustNot(m -> m
                            .wildcard(t -> t   
                                .field(FIELD_NAME)  
                                .caseInsensitive(false)
                                .value("*" + query + "*")
                            )
                        )
                    )
                ),
                IndexValueObject.class      
            );

            result = processResponce(response);
        } catch (ElasticsearchException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> startsWith(String query) {
        Collection<Integer> result = new ArrayList<Integer>();
        try {
            
            SearchResponse<IndexValueObject> response = esClient.search(s -> s
                .index(this.ELASTIC_INDEX_NAME) 
                .query(q -> q      
                    .prefix(t -> t   
                        .field(FIELD_NAME)  
                        .caseInsensitive(false)
                        .value(query)
                    )
                ),
                IndexValueObject.class      
            );

            result = processResponce(response);
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> notStartsWith(String query) {
        Collection<Integer> result = new ArrayList<Integer>();
        try {
            
            SearchResponse<IndexValueObject> response = esClient.search(s -> s
                .index(this.ELASTIC_INDEX_NAME) 
                .query(q -> q
                    .bool(b -> b
                        .mustNot(m -> m
                            .prefix(t -> t   
                                .field(FIELD_NAME)  
                                .caseInsensitive(false)
                                .value(query)        
                            )
                        )
                    )
                ),
                IndexValueObject.class
            );

            result = processResponce(response);
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> endsWith(String query) {
        Collection<Integer> result = new ArrayList<Integer>();
        try {
            
            SearchResponse<IndexValueObject> response = esClient.search(s -> s
                .index(this.ELASTIC_INDEX_NAME) 
                .query(q -> q
                    .regexp(t -> t   
                        .field(FIELD_NAME)  
                        .caseInsensitive(false)
                        .value(".*" + query)        
                    )
                ),
                IndexValueObject.class      
            );

            result = processResponce(response);
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Collection<Integer> notEndsWith(String query) {
        Collection<Integer> result = new ArrayList<Integer>();
        try {
            
            SearchResponse<IndexValueObject> response = esClient.search(s -> s
                .index(this.ELASTIC_INDEX_NAME) 
                .query(q -> q
                    .bool(b -> b
                        .mustNot(m -> m
                            .regexp(t -> t   
                                .field(FIELD_NAME)  
                                .caseInsensitive(false)
                                .value(".*" + query)        
                            )
                        )
                    )
                ),
                IndexValueObject.class      
            );

            result = processResponce(response);
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private Collection<Integer> processResponce(SearchResponse<IndexValueObject> response) {
        Collection<Integer> result = new ArrayList<Integer>();
        List<Hit<IndexValueObject>> hits = response.hits().hits();
        for (Hit<IndexValueObject> hit: hits) {
            IndexValueObject item = hit.source();
            result.add(Integer.parseInt(item.id));
        }
        return result;
    }

    public void close() {
        try {
            this.restClient.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void bulkInsert(List<IndexValueObject> documents) {
        try {
            BulkRequest.Builder br = new BulkRequest.Builder();

            for (IndexValueObject document : documents) {
                br.operations(op -> op           
                    .index(idx -> idx            
                        .index(ELASTIC_INDEX_NAME)       
                        .id(document.id)
                        .document(document)
                    )
                );
            }

            this.esClient.bulk(br.build());
            this.numberOfItems += documents.size();
            System.out.println("Number of documents: " + this.numberOfItems);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ElasticIndex elasticIndex = new ElasticIndex();
        // elasticIndex.createIndex();

        // List<IndexValueObject> result = new ArrayList<IndexValueObject>();
        // result.add(new IndexValueObject("1", "Paulp Alto"));
        // elasticIndex.bulkInsert(result);

    //     List<Product> products = fetchProducts();

    //     BulkRequest.Builder br = new BulkRequest.Builder();

    //     //for (Product product : products) {
    //         br.operations(op -> op           
    //             .index(idx -> idx            
    //                 .index("products")       
    //                 .id("1").
    //                 .document(product)
    //             )
    //         );
    //    // }

    //     BulkResponse result = esClient.bulk(br.build());
        ///elasticIndex.dropIndex("test");
        Collection <Integer> documents = elasticIndex.includes("Alto");
        for (Integer document : documents) {
            System.out.println(document);
        }
        System.out.println("Result:" + documents.size());
        elasticIndex.close();
        return;
    }
}
