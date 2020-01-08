package com.example.demo.dao;

import com.example.demo.pojo.Fruit;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface FruitRepository extends ElasticsearchRepository<Fruit,Integer> {
}
