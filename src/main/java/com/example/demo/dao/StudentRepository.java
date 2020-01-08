package com.example.demo.dao;

import com.example.demo.pojo.Student;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface StudentRepository extends ElasticsearchRepository<Student,Integer> {
}
