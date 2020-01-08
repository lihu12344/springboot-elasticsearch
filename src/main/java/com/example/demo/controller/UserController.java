package com.example.demo.controller;

import com.example.demo.dao.UserRepository;
import com.example.demo.pojo.User;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.ScrolledPage;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@RestController
public class UserController {

    @Resource
    private UserRepository userRepository;

    @Resource
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    @RequestMapping("/save4")
    public String save(){
        for (int i=0;i<1000;i++){
            User user=new User();

            user.setId(i);
            user.setName("瓜田李下 "+i);
            user.setAge(i%20+15);

            userRepository.save(user);
        }

        return "success";
    }

    @RequestMapping("/get8")
    public List<User> get(){
        QueryBuilder queryBuilder= QueryBuilders.rangeQuery("age").gte(32);

        NativeSearchQuery nativeSearchQuery=new NativeSearchQueryBuilder()
                .withQuery(queryBuilder)
                .withPageable(PageRequest.of(0,20))
                .build();

        long scrollTimeout = 10000;
        ScrolledPage<User> scrolledPage=elasticsearchRestTemplate.startScroll(scrollTimeout,nativeSearchQuery,User.class);
        System.out.println("总记录数为："+scrolledPage.getTotalElements());

        List<User> resultList=new ArrayList<>();
        while (scrolledPage.hasContent()){
            List<User> list=scrolledPage.getContent();
            resultList.addAll(list);

            System.out.println(list);
            System.out.println("当前页的条数为："+list.size()+"\n");

            scrolledPage=elasticsearchRestTemplate.continueScroll(scrolledPage.getScrollId(), scrollTimeout,User.class);
        }

        return resultList;
    }
}
