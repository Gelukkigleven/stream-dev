package com.retailersv1.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.retailersv1.domain.CartAddUuBean
 * @Author hou.dz
 * @Date 2025/1/3 9:07
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
