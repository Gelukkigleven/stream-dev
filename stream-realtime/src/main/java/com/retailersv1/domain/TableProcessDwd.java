package com.retailersv1.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package:
 * @Author:com.dz
 * @Date:2024/12/26 9:55
 * @description:
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDwd{

    // 来源表名
    String sourceTable;

    // 来源类型
    String sourceType;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    String op;
}
