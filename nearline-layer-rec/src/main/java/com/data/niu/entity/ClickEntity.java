package com.data.niu.entity;


/**
 * Created by apple on 2019/10/3.
 * 用户点击数据字段
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClickEntity {
    public String Visitor_id;
    public int user_id;
    public String time;
    public String click_tpye;
    public int product_id;
}
