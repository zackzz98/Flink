转换算子
map,filter,flatMap
map:一对一
filter:过滤
flatMap:一对多

聚合算子
sum,min,minBy,max,maxBy,reduce
sum:求和
min,minBy,max,maxBy: min返回的是最小值,minBy返回的是该字段中具有最小值的元素(max,maxBy同理)
reduce:当流中只有一条数据时,不会执行reduce

富函数:
1.所有Flink函数类都有其Rich版本
2.富函数有一个生命周期的概念
    open() 初始化
    close() 结束
    getRuntimeContext() 提供函数执行的并行度,任务名字,state状态等

分区算子 (学习文档：https://www.hangge.com/blog/cache/detail_3841.html)
shuffle(随机分区)：将上游数据随机分发到下游算子实例的每个分区中
    (底层对应的是ShufflePartitioner这个类，类里面有一个selectChannel函数，这个函数会计算数据将会被发送哪个分区，里面是random.nextInt，是随机的)
rebalance(重平衡分区)：将输出元素以轮询方式均匀地分布到下一个算子的实例中,有助于均匀地分布数据
    (底层对应的是RebalancePartitioner这个类，类里面有一个setup和selectChannel函数，setup函数会根据分区数初始化一个随机值nextChannelToSendTo，
    然后 selectChannel 函数会使用 nextChannelToSendTo 加 1 和分区数取模，把计算的值再赋给 nextChannelToSendTo ，
    后面以此类推，其实就可以实现向下游算子实例的多个分区循环发送数据了，这样每个分区获取到的数据基本一致)
rescale(重分区)：将输出元素以轮询的方式均匀地分布下一个算子的实例子集
    (底层对应的是RescalePartitioner这个类类里面有一个 selectChannel 函数，这里面的 numberOfChannels 是分区数量，其实也可以认为是我们所说的算子的并行度，因为一个分区是由一个线程负责处理的，它们两个是一一对应的)
broadcast(广播分区)：将输出元素广播到下一个算子的每个并行实例，适合于大数据集Join小数据集的场景，可以提高性能
    (底层对应的是 BroadcastPartitioner 这个类。看这个类中的 selectChannel 函数代码的注释，提示广播分区不支持选择 Channel，因为会输出数据到下游的每个 Channel 中，就是发送到下游算子实例的每个分区中)
global(全局分区)：将所有数据发送到下游的一个并行子任务中
    (该方法使用 GlobalPartitioner 分区程序来设置 DataStream 的分区，以便将输出值都转到下一个处理操作符（算子）的第一个实例。使用此设置时要小心，因为它可能会在应用程序中造成严重的性能瓶颈。)
keyBy(按键分区)：根据 key 的分组索引选择目标通道，将输出元素发送到相对应的下游分区
    (使用 KeyGroupStreamPartitioner 分区程序来设置 DataStream 的分区)
forward(转发分区)：将输出元素被转发到下一个操作（算子）的本地子任务
    (在上下游的算子没有指定分区器的情况下，如果上下游的算子并行度一致，则使用 ForwardPartitioner，否则使用 RebalancePartitioner。
    对于 ForwardPartitioner，必须保证上下游算子并行度一致，即上有算子与下游算子是 1 对 1 的关系，否则会抛出异常。)
    (forward 方法使用 ForwardPartitioner 分区程序来设置 DataStream 的分区，仅将元素转发到本地运行的下游操作（算子）)
custom partition(自定义分区)：自定义分区策略的 API 为 CustomPartitionerWrapper。该策略允许开发者自定义规则将上游算子元素发送到下游指定的算子实例中。
              使用该分区策略首先我们需要新建一个自定义分区器，然后使用这个自定义分区器进行分区。

分流：
filter：本质工作是过滤，如果通过filter进行分流的话，同一条流可能会被处理多次，效率低，所以一般不用
侧输出流：原理是对流中数据进行处理的时候，给元素打标签，所以使用侧输出流的时候，需要先创建标签对象，创建标签对象的时候，需要注意会存在泛型擦除问题
        (以匿名内部类方式创建对象，在创建对象时，指定泛型类型)
合流：
union：可以合并两条流或者多条流，参与合并的流的数据类型必须一致
connect：对两条流就行合并



Flink 时间语义
摄入时间：元素进入到Source算子的时间
事件时间：元素产生的时间
处理时间：元素到了算子,算子对其进行处理的时间
注：Flink1.12开始,默认的时间语义是事件时间语义

窗口：
将无限的数据划分为有限的数据块
分类：
按照驱动形式分：时间窗口(按照时间定义窗口的起始以及结束)、计数窗口(按照窗口中元素的个数定义窗口的起始及结束)
按照数据划分分方式：滚动窗口、滑动窗口、会话窗口、全局窗口
··滚动窗口(窗口大小固定、窗口和窗口之间不会重叠、窗口和窗口是首尾相接、元素只会属于一个窗口)
··滑动窗口(窗口大小固定、窗口和窗口可能会重叠、窗口和窗口不是首尾相接、同一个元素会属于对个窗口、 窗口大小/滑动步长)
··会话窗口(窗口大小不是固定、窗口和窗口之间不会重叠、窗口和窗口不是首尾相接、窗口之间会存在时间间隔(size)、元素只会属于一个窗口)
··全局窗口(将流中数据放到同一个窗口中,默认情况下,是没有结束的、需要手动指定触发器,指定窗口的结束时机、计数窗口的底层就是全局窗口)

窗口API
是否在开窗前进行了KeyBy
keyBy:针对keyBy之后的每一个组进行开窗,组和组之间相互不影响
··时间窗口 window()、windowAll()
··计数窗口 countWindow()、countWindowAll(窗口大小)、countWindowAll(窗口大小,滑动步长)
no-keyBy:针对整条流进行开窗,相当于并行度设置为1
··时间窗口 windowAll()
··计数窗口 countWindowAll(窗口大小)、countWindowAll(窗口大小,滑动步长)

窗口分配器
        开什么类型的窗口
        计数窗口
            滚动计数窗口
                countWindow[All](窗口大小)
            滑动计数窗口
                countWindow[All](窗口大小,滑动步长)
        时间窗口
            .window()
            .windowAll()
            滚动处理时间窗口
                TumblingProcessingTimeWindows
            滑动处理时间窗口
                SlidingProcessingTimeWindows
            处理时间会话窗口
                ProcessingTimeSessionWindows
            滚动事件时间窗口
                TumblingEventTimeWindows
            滑动事件时间窗口
                SlidingEventTimeWindows
            事件时间会话窗口
                EventTimeSessionWindows
        全局窗口
            stream.keyBy(...)
                   .window(GlobalWindows.create());

        以滚动处理时间为例，说明窗口的生命周期
            窗口对象什么时候创建
                属于窗口的第一个元素到来的时候，创建窗口对象

            窗口对象的起始和结束时间
                起始时间:向下取整
                结束时间:起始时间  +  窗口大小
                [起始时间,结束时间)
                最大时间: 结束时间 - 1ms

            窗口什么时候触发计算和关闭
                系统时间到了窗口的最大时间

    窗口处理函数
        如何对窗口中数据进行处理
        增量处理
            窗口数据来一条计算一次，不会缓存数据  优点：省空间      缺点：不能获取更丰富的窗口信息
            reduce
                窗口中元素类型以及向下游传递的类型必须一致
                reduce(value1,value2)
                    value1:中间累加的结果
                    value2:新来的数据
                注意：如果窗口中只有一条数据，reduce方法不会被调用
            aggregate
                窗口中的元素类型、累加器类型以及向下游传递的类型可以不一致
                createAccumulator:窗口中第一条数据到来的时候执行
                add:窗口中每来一条数据都会执行
                getResult:窗口触发计算的时候执行的方法
                merge:只有会话才需要重写
        全量处理
            窗口数据到来的时候不会马上计算，等窗口触发计算的时候，在整体进行计算  优点：可以获取更丰富的窗口信息  缺点：费空间
            apply
            process
                更底层，可以通过上下文对象获取窗口对象以及其它一些信息

        在实际开发的过程中，可以增量 + 全量


    窗口触发器
        什么时候触发窗口的计算

    窗口移除器
        在窗口触发计算之后，在处理函数执行之前(之后)，需要执行的移除操作是什么


WaterMark(水位线)
    前提：事件时间语义
    用于衡量事件时间进展的标记
    是一个逻辑时钟
    水位线也会作为流中的元素向下游传递
    水位线的值是根据流中元素的事件时间得到的
    flink在做处理的时候, 认为水位线之前的数据都已经处理过了
    主要用于窗口的触发计算、关闭以及定时器的执行
    水位线是递增的, 不会变小(相当于时间不会倒流)
    为了处理流中的迟到数据, 设置水位线的延迟时间, 目的其实让窗口或者定时器延迟触发

Flink内置水位线生成策略
    单调递增
        WatermarkStrategy.<流中数据类型>forMonotonousTimestamps()
    有界乱序
        WatermarkStrategy.<流中数据类型>forBoundedOutOfOrderness(乱序程度)
    单调递增是有界乱序的子类
    单调递增是有界乱序的一种特殊情况, 乱序程度是0
    class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator {
        onEvent:流中每条数据到来的时候都会执行方法
            取出流中元素最大事件时间
        OnPeriodicEmit:周期性执行的方法, 默认周期200ms  可以通过如下代码修改默认周期  env.getConfig().setAutoWatermarkInterval()
            创建水位线对象, 发射到流中   wm = 最大时间 - 乱序程度 - 1ms
    }

以滚动事件时间窗口为例
    窗口对象什么时候创建
        当属于当前窗口的第一个元素进来的时候
    窗口的起始时间
        向下取整
    窗口的结束时间
        起始时间 + 窗口大小
    窗口的最大事件
        结束时间 - 1ms
    窗口什么时候触发计算
        水位线 >= 窗口最大时间
        (window.maxTimestamp()) <= ctx.getCurrentWatermark()
    窗口什么时候关闭
        水位线到了window.maxTimestamp() + allowedLateness

水位线的传递
    如果上游一个并行度, 下游多个并行度, 广播
    如果上游多个并行度, 下游一个并行度, 将上游所有并行度的水位线拿过来取最小
    如果上游多个并行度, 下游多个并行度, 先广播再取最小

迟到数据的处理
    指定wm的生成策略为有界乱序, 指定乱序程度(延迟窗口触发计算以及关闭时间)
    设置窗口的允许迟到时间
    侧输出流


FlinkAPI 双流Join
    基于窗口的实现
        滚动窗口
        滑动窗口
        会话窗口
        语法
            ds1
                .join(ds2)
                .where(提取ds1中关联字段)
                .equalTo(提取ds2中关联字段)
                .window()
                .apply()
    基于状态的实现
        intervalJoin
        语法
            KeyedA
                .intervalJoin(KeyedB)
                .between(下界,上界)
                .process()
        底层实现
            connect + 状态

        底层处理流程
            判断是否迟到
            将当前数据放到状态中缓存起来
            用当前这条数据和另外一条流中缓存的数据进行关联
            清除状态
    注意：FlinkAPI不管是通过哪种方式实现双流Join, 都支持内连接
         如果想要实现外连接效果, 可以通过FlinkSQL或者自己通过connect实现

处理函数
    处于Flink分层API的最底层
    可以更加灵活的对流中数据进行处理(处理逻辑全靠自己实现)
    处理函数不是接口,是抽象类,并且继承了AbstractRichFunction, 所以富函数拥有的功能, 处理函数都有
    一般在处理函数的方法如下：
        processElement: 对流中数据进行处理的方法,是抽象的
        onTimer: 定时器触发的时候执行的方法,非抽象的
处理函数分类:
    ProcessFunction
        最基本的处理函数,基于DataStream直接调用.process()时作为参数传入
    KeyedProcessFunction
        对流按键分组后的处理函数,基于KeyedStream调用.process()时作为参数传入。要想使用定时器,必须基于KeyedStream
    ProcessWindowFunction
        开窗之后的处理函数，也是全窗口函数的代表。基于WindowedStream调用.process()时作为参数传入。
    ProcessAllWindowFunction
        同样是开窗之后的处理函数，基于AllWindowedStream调用.process()时作为参数传入。
    CoProcessFunction
        合并（connect）两条流之后的处理函数，基于ConnectedStreams调用.process()时作为参数传入。关于流的连接合并操作，我们会在后续章节详细介绍。
    ProcessJoinFunction
        间隔连接（interval join）两条流之后的处理函数，基于IntervalJoined调用.process()时作为参数传入。
    BroadcastProcessFunction
        广播连接流处理函数，基于BroadcastConnectedStream调用.process()时作为参数传入。这里的“广播连接流”BroadcastConnectedStream，是一个未keyBy的普通DataStream与一个广播流（BroadcastStream）做连接（conncet）之后的产物。关于广播流的相关操作，我们会在后续章节详细介绍。
    KeyedBroadcastProcessFunction
        按键分组的广播连接流处理函数，同样是基于BroadcastConnectedStream调用.process()时作为参数传入。与BroadcastProcessFunction不同的是，这时的广播连接流，是一个KeyedStream与广播流（BroadcastStream）做连接之后的产物。

只有在KeyedProcessFunction的processElement方法中，才能使用定时服务
    在处理函数中，常用的操作
        获取当前分组的key
            ctx.getCurrentKey()
        获取当前元素的事件时间
            ctx.timestamp()
        将数据"放到"侧输出流中
            注意：如果要使用侧输出流，必须用process算子对流中数据进行处理，
            ctx.output()
        获取定时服务
            ctx.timerService()
        获取当前处理时间
             timerService.currentProcessingTime()
        获取当前水位线
            timerService.currentWatermark()
        注册处理时间定时器   定时器的触发是由系统时间触发的
            timerService.registerProcessingTimeTimer(currentProcessingTime + 10000)
        注册事件时间定时器   定时器的触发是由水位线(逻辑时钟)触发的
            timerService.registerEventTimeTimer(10)
        删除处理时间定时器
            timerService.deleteProcessingTimeTimer(10);
        删除事件时间定时器
            timerService.deleteEventTimeTimer(10)


状态
    用于保存程序运行的中间结果
状态分类
    原始状态
        由程序员自己开辟内存，自己负责状态的序列化以及容错恢复等
    托管状态
        由Flink框架管理状态的存储、序列化以及容错恢复等
        算子状态
            作用范围: 算子的每一个并行子任务上(分区、并行度、slot)
            ListState
            UnionListState
            BroadcastState(重点)
            使用步骤
                类必须实现checkpointedFunction接口
                    initializeState:初始化状态
                    snapshotState:对状态进行备份
                算子状态和普通成员变量声明的位置一样，作用范围范围一样，但是不同的是算子状态可以被持久化
                其实算子状态底层在snapshotState就是将普通的变量放到状态中进行的持久化，相当于普通的变量也被持久保存了


        键控状态
            作用范围:经过keyBy之后的每一个组，组和组之间状态是隔离的
            ValueState
            ListState
            MapState
            ReducintState
            AggregatingState
            使用步骤
                在处理函数类成员变量位置声明状态，注意：虽然在成员变量位置声明，但是作用范围是keyBy后的每一个组
                在open方法中对状态进行初始化
                在具体的处理函数中使用状态

广播状态BroadcastState
    也算是算子状态的一种，作用范围也是算子子任务
    广播状态的使用方式是固定的

状态后端
    管理本地状态的存储方式和位置以及检查点的存储位置
    检查点是对状态做的备份，是状态的一个副本
    Flink1.13前
                                      状态                     检查点
        Memory                      TM堆内存                 JM的堆内存
        Fs                          TM堆内存                 文件系统
        RocksDB                     RocksDB库                文件系统

    从Flink1.13开始
                                      状态                     检查点
        HashMap                     TM堆内存               JM的堆内存|文件系统
        RocksDB                     RocksDB库              文件系统

状态
    用于保存程序运行的中间结果
检查点
    对状态的备份
    是状态的快照(副本)
    检查点是周期性的对状态进行备份，周期可以自己设置

检查点备份时间
    一条数据被程序的所有算子任务都处理过后再进行备份

检查点底层算法
    异步分界线快照算法
    核心：分界线barrier
    原理：当到了检查点的备份周期后，在JobManager上有一个叫检查点协调器的组件，它会向各个Source子任务发送一个检查点分界线barrier
         然后Source算子上的状态会进行备份，barrier也会作为流中的一个元素，随着流的流动向下游传递，当barrier到了其它的transform算子，
         transform算子的状态也会进行备份，当barrier到了了sink后，sink上状态也会进行备份。
         当barrier已经从source走完sink了，说明barrier前的元素一定已经从source走完sink了，这个时候一次检查点备份完成

         当程序遇到故障重启的时候，会从上次最完整的检查点中恢复状态数据

检查点分界线的传递
    如果上游一个并行度，下游是多个并行度，广播
    如果上游多个并行度，下游是一个并行度，对齐或者非对齐
    如果上游是多个并行度，下游也是多个并行度，先广播再对齐或者非对齐

检查点分界线barrier算子
    barrier对齐的精准一次
        如果下游算子的数据来源于上游多个不同的并行度，需要等待上游各个并行度上barrier都到达下游算子任务后，才会对下游算子任务状态进行备份
        在等到的过程中，如果barrier已经到达并行度上又有数据过来，不会对其进行处理，只是将数据放到缓存中，等barrier对齐后，再对数据进行处理
        优点：保证一致性        缺点：时效性差
    barrier对齐的至少一次
        如果下游算子的数据来源于上游多个不同的并行度，需要等待上游各个并行度上barrier都到达下游算子任务后，才会对下游算子任务状态进行备份
        在等到的过程中，如果barrier已经到达并行度上又有数据过来，直接会对新来的数据进行处理，新来的数据如果状态有影响的话，当出现故障重启重启
        后，数据会被重复处理
        优点：时效性好        缺点：数据可能重复
    非barrier对齐的精准一次
        如果下游算子的数据来源于上游多个不同的并行度，当上游的并行度上又barrier过来的时候
        直接将当前barrier跳转到下游算子输出缓冲区末端
        标记当前barrier跳过的数据以及其它未到达的barrier之前的数据
        将标记的数据以及状态都进行备份

        优点：时效性好 + 保证一致性    缺点：保存的内容变多  状态 + 数据

检查点相关的设置
    启用检查点
    检查点存储
    检查点模式（CheckpointingMode）
    超时时间（checkpointTimeout）
    最小间隔时间（minPauseBetweenCheckpoints）
    最大并发检查点数量（maxConcurrentCheckpoints）
    开启外部持久化存储（enableExternalizedCheckpoints）
    检查点连续失败次数（tolerableCheckpointFailureNumber）
    非对齐检查点（enableUnalignedCheckpoints）
    对齐检查点超时时间（alignedCheckpointTimeout）

    通用增量 checkpoint (changelog)
    最终检查点

保存点
    和检查点一样，也是对状态进行备份的
    底层算法一样
    检查点是程序周期性的对状态进行备份；保存点是由程序员手动的对状态进行备份