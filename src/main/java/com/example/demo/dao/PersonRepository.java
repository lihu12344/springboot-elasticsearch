package com.example.demo.dao;

import com.example.demo.pojo.Person;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface PersonRepository extends ElasticsearchRepository<Person,Integer> {

    List<Person> findByName(String name);
    List<Person> findByAge(Integer age);
    List<Person> findByAgeBetween(Integer min,Integer max);
}
