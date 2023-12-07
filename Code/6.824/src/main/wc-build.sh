# wc.so运行前每次都要重新build,因此在写个wc-build.sh脚本放在main路径下面 configuration取名"build wc"
# 进行并发检测，并将编译后生成的wc.so插件，以参数形式加入mrsequential.go,并运行
go build -race -buildmode=plugin ../mrapps/wc.go
# 删除生成的mr-out*以免每次第二次运行得先删除
rm mr-out*
