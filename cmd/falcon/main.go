// falcon 搜索引擎节点入口
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FalconEngine/falcon/api"
	"github.com/FalconEngine/falcon/config"
	"github.com/FalconEngine/falcon/index"
	"github.com/FalconEngine/falcon/node"
	"github.com/FalconEngine/falcon/pkg/mlog"
	"github.com/FalconEngine/falcon/transport"

	// 显式引入全部内置插件（字段类型/分词器/打分器/查询子句/聚合）
	_ "github.com/FalconEngine/falcon/plugins"
)

var configPath = flag.String("config", "", "配置文件路径（yaml）")

func main() {
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config error: %v\n", err)
		os.Exit(1)
	}

	switch cfg.Log.Level {
	case "debug":
		mlog.SetLevel(mlog.LevelDebug)
	case "warn":
		mlog.SetLevel(mlog.LevelWarn)
	case "error":
		mlog.SetLevel(mlog.LevelError)
	}

	mlog.Info("falcon node [ %s ] starting ... http:%d grpc:%d data:%s master:%v data:%v",
		cfg.Node.Name, cfg.HTTP.Port, cfg.GRPC.Port, cfg.Data.Path, cfg.Node.Master, cfg.Node.Data)

	// 打开索引管理器（扫描数据目录，加载已有索引并回放 translog）
	mgr, err := index.OpenManager(cfg.Data.Path)
	if err != nil {
		mlog.Error("open index manager error: %v", err)
		os.Exit(1)
	}

	// 装配集群控制面与节点间传输
	nd := node.New(cfg, mgr, transport.NewClient())
	grpcServer := transport.NewServer(nd)
	go func() {
		addr := fmt.Sprintf(":%d", cfg.GRPC.Port)
		mlog.Info("falcon node [ %s ] grpc listening on %s", cfg.Node.Name, addr)
		if err := grpcServer.Start(addr); err != nil {
			mlog.Error("grpc server error: %v", err)
			os.Exit(1)
		}
	}()
	if err := nd.Start(); err != nil {
		mlog.Error("join cluster error: %v", err)
		os.Exit(1)
	}

	// 启动 REST 服务
	server := api.NewServer(mgr)
	server.SetCluster(nd)
	go func() {
		addr := fmt.Sprintf(":%d", cfg.HTTP.Port)
		mlog.Info("falcon node [ %s ] http listening on %s", cfg.Node.Name, addr)
		if err := server.Start(addr); err != nil && err != http.ErrServerClosed {
			mlog.Error("http server error: %v", err)
			os.Exit(1)
		}
	}()

	// TODO(5b): 复制流、scatter-gather 远端转发、故障转移

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	// 优雅退出：先停 HTTP/gRPC，再停 raft，最后关闭全部引擎
	mlog.Info("falcon node [ %s ] shutting down ...", cfg.Node.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		mlog.Warn("http shutdown error: %v", err)
	}
	grpcServer.GracefulStop()
	nd.Stop()
	if err := mgr.Close(); err != nil {
		mlog.Warn("close index manager error: %v", err)
	}
	mlog.Info("falcon node [ %s ] stopped", cfg.Node.Name)
}
