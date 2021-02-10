package org.example.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desription:
 *
 * @ClassName Order
 * @Author Zhanyuwei
 * @Date 2021/2/10 4:06 下午
 * @Version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {

    private String orderId;
    private Integer userId;
    private Integer money;
    private Long eventTime;
}
