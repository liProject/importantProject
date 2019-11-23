package flink.checkpointedState

import java.{lang, util}

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

object OperaState {
  def main(args: Array[String]): Unit = {
    /**
      * 1.获取执行环境
      * 2.设置检查点机制：路径，重启策略
      * 3.自定义数据源
      * （1）需要继承并行数据源和CheckpointedFunction
      * （2）设置listState,通过上下文对象context获取
      * (3)数据处理，保留offset
      * (4)制作快照
      * 4.数据打印
      * 5.触发执行
      */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //设置检查点保存路径
    env.setStateBackend(new FsStateBackend("hdfs://hadoop4:8020/checkpoint"))
    //开启检查点
    env.enableCheckpointing(1000)
    //强一致性，只消费一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //检查点触发的时间间隔
    env.getCheckpointConfig.setCheckpointInterval(1000)
    //检查点制作的超时时间
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //检查点制作失败是，程序是否终止，默认是true，这里要改成false
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //制作检查点的最大线程数
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //设置如果任务取消，保留检查点
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置固定重启策略，每次重启间隔为5S，重启3次，没有成功，就不再重启
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)))

    /**
      * def addSource[T: TypeInformation](
      * function: SourceFunction[T]): DataStream[T]
      */
    env.addSource(new MySourceFunc)
        .print()

    env.execute()
  }
}

/**
  * 制作检查点函数
  */
class MySourceFunc extends SourceFunction[Long] with CheckpointedFunction {

  //检查点状态
  var checkpointedState: ListState[Long] = _

  //偏移量
  var numElementsEmitted = 0L

  /**
    * 业务逻辑处理的主方法
    * @param ctx
    */
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {

    //通过checkpointedState获取最新偏移量
    val lsStateList: util.Iterator[Long] = checkpointedState.get().iterator()
    while (lsStateList.hasNext) {
      numElementsEmitted = lsStateList.next()
    }

    while (true) {
      numElementsEmitted += 1
      Thread.sleep(1000)
      ctx.collect(numElementsEmitted)
      if (numElementsEmitted > 10) {
        1 / 0
      }
    }

  }

  override def cancel(): Unit = ???

  /**
    * 制作检查点
    *
    * @param context
    */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
//    Preconditions.checkState(this.checkpointedState != null,
//      "The " + getClass().getSimpleName() + " has not been properly initialized.")

    this.checkpointedState.clear()
    this.checkpointedState.add(this.numElementsEmitted)
  }

  /**
    * 初始化状态
    *
    * @param context
    */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    /**
      * public ListStateDescriptor(
      * String name, TypeInformation<T> elementTypeInfo)
      */
    val lsState: ListStateDescriptor[Long] = new ListStateDescriptor[Long](
      "ls",
      TypeInformation.of(new TypeHint[Long] {})
    )

    checkpointedState = context.getOperatorStateStore.getListState(lsState)
  }
}
