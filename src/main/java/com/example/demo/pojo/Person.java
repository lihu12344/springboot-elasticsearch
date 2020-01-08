package com.example.demo.pojo;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.data.elasticsearch.annotations.MultiField;

@Data
@Document(indexName = "person")
public class Person {

    @Id
    @Field(type = FieldType.Integer)
    private Integer id;

    @MultiField(mainField = @Field(type = FieldType.Keyword))
    private String name;

    private Integer age;
}
