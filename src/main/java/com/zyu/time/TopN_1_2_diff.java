package com.zyu.time;

/**
 * @author zyu
 * date2025/3/25 11:00
 */
public class TopN_1_2_diff {
    /*
    * TODO
    *  实现方式:
    *   TopN_1中使用ProcessAllWindowFunction 直接处理窗口中的所有元素,统计水位值的出现次数并输出TopN
    *   TopN_2中使用AggregateFunction和ProcessWindowFunction, 先对每个水位值进行聚合统计, 然后在窗口结束时处理结果
    *  处理逻辑:
    *   TopN_1在处理窗口数据时, 使用了一个Map来统计水位值的出现次数,最后进行排序
    *   TopN_2则通过AggregateFunction进行计数, 使用ProcessWindowFunction 处理聚合后的结果,输出更为灵活
    *  定时器使用:
    *   TopN_2 使用了定时器来确保在窗口结束时输出结果，适合处理延迟数据和确保数据完整性
    *
    *
    * */
}