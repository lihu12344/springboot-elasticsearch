package com.example.demo.controller;

import com.example.demo.dao.FruitRepository;
import com.example.demo.pojo.Fruit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregationBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@RestController
public class FruitController {

    @Resource
    private FruitRepository fruitRepository;

    @Resource
    private ElasticsearchRestTemplate elasticsearchTemplate;

    @RequestMapping("/save2")
    public String save(){
        for(int i=0;i<10;i++){
            Fruit fruit=new Fruit();
            fruit.setId(i);

            switch (i%3){
                case 0: fruit.setName("苹果"); break;
                case 1: fruit.setName("香蕉"); break;
                case 2: fruit.setName("栗子");
            }
            fruit.setPrice(i%10+2);

            fruitRepository.save(fruit);
        }

        return "success";
    }

    @RequestMapping("/aggregation")
    public void aggregation(){
        QueryBuilder queryBuilder= QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("price").gte(5));

        TermsAggregationBuilder aggregationBuilder=AggregationBuilders.terms("fruit_name").field("name");

        Map<String,String> map=new HashMap<>();
        map.put("count","_count");
        map.put("min","min_price");
        BucketSelectorPipelineAggregationBuilder pipelineAggregationBuilder= PipelineAggregatorBuilders
                .bucketSelector("having_count",map,new Script("params.count>1&&params.min>5"));

        AggregationBuilder minAggregation=AggregationBuilders.min("min_price").field("price");
        AggregationBuilder maxAggregation=AggregationBuilders.max("max_price").field("price");
        AggregationBuilder avgAggregation=AggregationBuilders.avg("avg_price").field("price");

        aggregationBuilder.subAggregation(minAggregation)
                .subAggregation(maxAggregation)
                .subAggregation(avgAggregation)
                .subAggregation(pipelineAggregationBuilder);
        NativeSearchQuery nativeSearchQuery=new NativeSearchQueryBuilder().withQuery(queryBuilder)
                .addAggregation(aggregationBuilder).build();

        AggregatedPage<Fruit> aggregatedPage=elasticsearchTemplate.queryForPage(nativeSearchQuery,Fruit.class);

        Terms terms =(Terms) aggregatedPage.getAggregation("fruit_name");
        for (Terms.Bucket bucket: terms.getBuckets()){
            Aggregations aggregations=bucket.getAggregations();
            System.out.print("bucket："+bucket.getKeyAsString()+" ");

            Min min= aggregations.get("min_price");
            Max max= aggregations.get("max_price");
            Avg avg= aggregations.get("avg_price");
            System.out.println("min："+min.getValue()+" "+"max："+max.getValue()+" "+"avg："+avg.getValue());
        }

    }

    @RequestMapping("/aggregation2")
    public void aggregation2(){
        QueryBuilder queryBuilder= QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("price").gte(5));

        SumAggregationBuilder sumAggregationBuilder= AggregationBuilders.sum("sum_price").field("price");
        MinAggregationBuilder minAggregationBuilder=AggregationBuilders.min("min_price").field("price");
        MaxAggregationBuilder maxAggregationBuilder=AggregationBuilders.max("max_price").field("price");
        AvgAggregationBuilder avgAggregationBuilder=AggregationBuilders.avg("avg_price").field("price");
        StatsAggregationBuilder statsAggregationBuilder=AggregationBuilders.stats("stats_price").field("price");

        NativeSearchQuery nativeSearchQuery=new NativeSearchQueryBuilder()
                .withQuery(queryBuilder)
                .addAggregation(sumAggregationBuilder)
                .addAggregation(minAggregationBuilder)
                .addAggregation(maxAggregationBuilder)
                .addAggregation(avgAggregationBuilder)
                .addAggregation(statsAggregationBuilder).build();

        AggregatedPage<Fruit> aggregatedPage=elasticsearchTemplate.queryForPage(nativeSearchQuery,Fruit.class);
        Sum sum=(Sum) aggregatedPage.getAggregation("sum_price");
        Min min=(Min) aggregatedPage.getAggregation("min_price");
        Max max=(Max) aggregatedPage.getAggregation("max_price");
        Avg avg=(Avg) aggregatedPage.getAggregation("avg_price");
        Stats stats=(Stats) aggregatedPage.getAggregation("stats_price");

        System.out.println("sum："+sum.getValue());
        System.out.println("min："+min.getValue());
        System.out.println("max："+max.getValue());
        System.out.println("avg："+avg.getValue());
        System.out.println("sum："+stats.getSumAsString()+"  "+
                "min："+stats.getMinAsString()+" "+
                "max："+stats.getMaxAsString()+" "+
                "avg："+stats.getAvgAsString()+" "+
                "count："+stats.getCount());
    }

    @RequestMapping("/aggregation3")
    public void aggregation3(){
        TermsAggregationBuilder aggregationBuilder=AggregationBuilders.terms("fruit_name").field("name");

        Map<String,String> map=new HashMap<>();
        map.put("min","min_price");
        BucketSelectorPipelineAggregationBuilder pipelineAggregationBuilder= PipelineAggregatorBuilders
                .bucketSelector("having_count",map,new Script("params.min>0"));

        AggregationBuilder minAggregation=AggregationBuilders.min("min_price").field("price");
        aggregationBuilder.subAggregation(minAggregation).subAggregation(pipelineAggregationBuilder);

        NativeSearchQuery nativeSearchQuery=new NativeSearchQueryBuilder()
                .addAggregation(aggregationBuilder).build();

        AggregatedPage<Fruit> aggregatedPage=elasticsearchTemplate.queryForPage(nativeSearchQuery,Fruit.class);

        Terms terms =(Terms) aggregatedPage.getAggregation("fruit_name");
        for (Terms.Bucket bucket: terms.getBuckets()){
            Aggregations aggregations=bucket.getAggregations();
            System.out.print("bucket："+bucket.getKeyAsString()+" ");

            Min min= aggregations.get("min_price");
            System.out.println("min："+min.getValue());
        }
    }
}