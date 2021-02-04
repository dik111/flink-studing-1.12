package org.example.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desription:
 *
 * @ClassName Student
 * @Author Zhanyuwei
 * @Date 2021/2/4 9:06 下午
 * @Version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {

    private Integer id;

    private String name;

    private Integer age;
}
