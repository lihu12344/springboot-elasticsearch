package com.example.demo.controller;

import com.example.demo.dao.StudentRepository;
import com.example.demo.pojo.School;
import com.example.demo.pojo.Student;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

@RestController
public class StudentController {

    @Resource
    private StudentRepository studentRepository;

    @RequestMapping("/save3")
    public String save(){
        for (int i=0;i<10;i++){
            Student student=new Student();
            student.setId(i);
            student.setName("瓜田李下 "+i);
            student.setAge(i%10+2);

            School school=new School();
            switch (i%3){
                case 0: {
                    school.setId(i);
                    school.setName("南天门");
                }   break;

                case 1: {
                    school.setId(i);
                    school.setName("北天门");
                }  break;

                case 2: {
                    school.setId(i);
                    school.setName("冬天门");
                }  break;
            }
            student.setSchool(school);

            studentRepository.save(student);
        }

        return "success";
    }

    @RequestMapping("/get5")
    public List<Student> get(){
        QueryBuilder queryBuilder= QueryBuilders.nestedQuery("school",
                QueryBuilders.termQuery("school.name","南天门"), ScoreMode.Avg);

        NativeSearchQuery nativeSearchQuery=new NativeSearchQueryBuilder().withQuery(queryBuilder).build();
        List<Student> list= studentRepository.search(nativeSearchQuery).getContent();
        list.forEach(System.out::println);

        return list;
    }

    @RequestMapping("/get6")
    public List<Student> get2(){
        QueryBuilder queryBuilder=QueryBuilders.nestedQuery("school",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("school.name","南天门"))
                        .must(QueryBuilders.rangeQuery("school.id").gte(5)),ScoreMode.Avg);

        NativeSearchQuery nativeSearchQuery=new NativeSearchQueryBuilder().withQuery(queryBuilder).build();
        List<Student> list= studentRepository.search(nativeSearchQuery).getContent();
        list.forEach(System.out::println);

        return list;
    }
}