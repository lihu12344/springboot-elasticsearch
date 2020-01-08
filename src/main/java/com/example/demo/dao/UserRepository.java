package com.example.demo.dao;

import com.example.demo.pojo.User;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface UserRepository extends ElasticsearchRepository<User,Integer> {
}
