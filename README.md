这个流程采用生产者消费者模式，会在下载的同时执行解压、数据处理和上传任务。

使用方法：

1. 执行下载

python batch_download_unbalanced_train.py <页码> <批次>

这个hhl来指定每个人运行哪个页码哪个批次。

2. 执行流水线

新开一个terminal

python async_pipeline_processor.py --page 3 --batch 2 --token your_token_here

token由hhl定期更新在群里。
