package com.example.demo.controller;

import com.example.demo.dao.PersonRepository;
import com.example.demo.pojo.Person;
import org.apache.commons.collections4.IteratorUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
public class PersonController {

    @Resource
    private PersonRepository personRepository;

    @Resource
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    @RequestMapping("/save")
    public String save(){
        for (int i=0;i<100;i++){
            Person person=new Person();
            person.setId(i);
            person.setName("瓜田李下 "+i);
            person.setAge(i/10+10);

            personRepository.save(person);
        }

        return "success";
    }

    @RequestMapping("/get")
    public List<Person> getByName(@RequestParam("name") String name){
        return personRepository.findByName(name);
    }

    @RequestMapping("/get2")
    public List<Person> getByAge(@RequestParam("age") Integer age){
        return personRepository.findByAge(age);
    }

    @RequestMapping("/get3")
    public List<Person> getBYAgeBetween(@RequestParam("min") Integer min,@RequestParam("max") Integer max){
        return personRepository.findByAgeBetween(min,max);
    }

    @RequestMapping("/get4")
    public List<Person> getByBoolQuery(@RequestParam(name = "name",required = false) String name,@RequestParam(name = "age",required = false) Integer age){
        if (name==null&&age==null){
            return IteratorUtils.toList(personRepository.findAll().iterator());
        }

        NativeSearchQueryBuilder nativeSearchQueryBuilder=new NativeSearchQueryBuilder();
        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();

        if (name!=null){
            boolQueryBuilder.must(QueryBuilders.termQuery("name",name));
        }

        if (age!=null){
            boolQueryBuilder.must(QueryBuilders.termQuery("age",age));
        }

        SearchQuery searchQuery=nativeSearchQueryBuilder.withQuery(boolQueryBuilder).build();
        return personRepository.search(searchQuery).getContent();
    }

    @RequestMapping("/getAll")
    public List<Person> get(){
        return personRepository.findAll(PageRequest.of(0,100,
                Sort.by(Sort.Direction.ASC,"age"))).getContent();
    }

    @RequestMapping("/update")
    public void update(){
        System.out.println("更新前数据为：");
        Person person=personRepository.findById(1).get();
        System.out.println(person);

        person.setName("海贼王");
        personRepository.save(person);

        System.out.println("更新后数据为：");
        System.out.println(personRepository.findById(1).get());
    }

    @RequestMapping("/count")
    public long count(){
        return personRepository.count();
    }

    @RequestMapping("/delete")
    public String delete(){
        personRepository.deleteById(1);

        return "success";
    }

    @RequestMapping("/highlight")
    @SuppressWarnings("unchecked")
    public List<Person> highlight(){
        QueryBuilder queryBuilder=QueryBuilders.regexpQuery("name","瓜田李下 1.");
        HighlightBuilder highlightBuilder=new HighlightBuilder().field("name")
                .preTags("<span style='color:red'>").postTags("</span>")
                .requireFieldMatch(false);

        NativeSearchQuery nativeSearchQuery=new NativeSearchQueryBuilder()
                .withQuery(queryBuilder)
                .withHighlightBuilder(highlightBuilder).build();

        AggregatedPage<Person> aggregatedPage=elasticsearchRestTemplate.queryForPage(nativeSearchQuery,
                Person.class, new SearchResultMapper() {
                    @Override
                    public <T> AggregatedPage<T> mapResults(SearchResponse searchResponse, Class<T> aClass, Pageable pageable) {
                        List<Person> list=new ArrayList<>();

                        SearchHits searchHits=searchResponse.getHits();
                        if (searchHits.getHits().length!=0){
                            for (SearchHit searchHit:searchHits){
                                Person person=new Person();

                                Map<String,Object> map=searchHit.getSourceAsMap();
                                person.setId(Integer.parseInt(map.get("id").toString()));
                                person.setAge(Integer.parseInt(map.get("age").toString()));

                                HighlightField nameField=searchHit.getHighlightFields().get("name");
                                if (nameField!=null){
                                    person.setName(nameField.getFragments()[0].toString());
                                }else {
                                    person.setName(map.get("name").toString());
                                }

                                list.add(person);
                            }
                        }

                        if (list.size()!=0){
                            return new AggregatedPageImpl<>((List<T>)list);
                        }

                        return null;
                    }

                    @Override
                    public <T> T mapSearchHit(SearchHit searchHit, Class<T> aClass) {
                        return null;
                    }
                });

        List<Person> list=aggregatedPage.getContent();
        list.forEach(System.out::println);

        return list;
    }
}
