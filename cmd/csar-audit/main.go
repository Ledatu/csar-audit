// Command csar-audit is the centralized audit ingest and query service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ledatu/csar-audit/internal/config"
	"github.com/ledatu/csar-audit/internal/consumer"
	"github.com/ledatu/csar-audit/internal/ingest"
	auditmetrics "github.com/ledatu/csar-audit/internal/metrics"
	"github.com/ledatu/csar-audit/internal/pipeline"
	"github.com/ledatu/csar-audit/internal/query"
	"github.com/ledatu/csar-audit/internal/rmq"
	"github.com/ledatu/csar-audit/internal/store"
	"github.com/ledatu/csar-core/configload"
	"github.com/ledatu/csar-core/gatewayctx"
	"github.com/ledatu/csar-core/health"
	"github.com/ledatu/csar-core/httpmiddleware"
	"github.com/ledatu/csar-core/httpserver"
	"github.com/ledatu/csar-core/logutil"
	"github.com/ledatu/csar-core/observe"
	"github.com/ledatu/csar-core/pgutil"
	"github.com/ledatu/csar-core/tlsx"
	auditv1 "github.com/ledatu/csar-proto/csar/audit/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

// Version is set at build time via ldflags.
var Version = "dev"

func main() {
	inner := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(logutil.NewRedactingHandler(inner))

	sf := configload.NewSourceFlags()
	sf.RegisterFlags(flag.CommandLine)

	otlpEndpoint := ""
	otlpInsecure := false
	flag.StringVar(&otlpEndpoint, "otlp-endpoint", otlpEndpoint, "OTLP gRPC endpoint (empty disables tracing)")
	flag.BoolVar(&otlpInsecure, "otlp-insecure", otlpInsecure, "insecure OTLP connection")
	flag.Parse()

	if err := run(sf, otlpEndpoint, otlpInsecure, logger); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(sf *configload.SourceFlags, otlpEndpoint string, otlpInsecure bool, logger *slog.Logger) error {
	ctx := context.Background()

	srcParams := sf.SourceParams()
	cfg, err := configload.LoadInitial(ctx, &srcParams, logger, config.LoadFromBytes)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ep := otlpEndpoint
	if ep == "" {
		ep = cfg.Tracing.Endpoint
	}
	sample := cfg.Tracing.SampleRatio
	if sample == 0 {
		sample = 1.0
	}

	tp, err := observe.InitTracer(ctx, observe.TraceConfig{
		ServiceName:    cfg.Service.Name,
		ServiceVersion: Version,
		Endpoint:       ep,
		SampleRate:     sample,
		Insecure:       otlpInsecure,
	})
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	defer func() { _ = tp.Close() }()

	reg := observe.NewRegistry()

	pool, err := pgutil.NewPool(ctx, cfg.Database.DSN, pgutil.WithLogger(logger.With("component", "postgres")))
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()

	pgStore := store.NewPostgres(pool, logger.With("component", "audit_store"))
	if err := pgStore.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	rmqCfg := rmq.ConnectionConfig{
		URL:            cfg.RabbitMQ.URL,
		ReconnectDelay: cfg.RabbitMQ.ReconnectDelay.Std(),
	}
	cm := rmq.NewConnectionManager(rmqCfg, logger.With("component", "rabbitmq"))
	if err := cm.Connect(ctx); err != nil {
		return fmt.Errorf("rabbitmq connect: %w", err)
	}
	defer func() { _ = cm.Close() }()

	if err := declareDLQ(cm, cfg.Consumer.DLQ.Name); err != nil {
		return fmt.Errorf("declare rabbitmq dlq: %w", err)
	}

	pub := rmq.NewPublisher(cm, cfg.Consumer.Queue.Name)

	var bufDepth func() float64
	m := auditmetrics.New(reg, func() float64 {
		if bufDepth != nil {
			return bufDepth()
		}
		return 0
	})

	buf := pipeline.NewBuffer(cfg.Ingest.BufferSize, cfg.Ingest.PublisherWorkers, pub, logger, &pipeline.BufferMetrics{
		EventsPublished: m.EventsPublished,
		EventsDropped:   m.EventsDropped,
	})
	defer buf.Close()
	bufDepth = func() float64 { return float64(buf.Depth()) }

	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()
	go consumer.Run(appCtx, cm, pgStore, cfg, logger, &consumer.ConsumerMetrics{
		BatchSize:         m.BatchSize,
		BatchFlushSeconds: m.BatchFlushSeconds,
		EventsWritten:     m.EventsWritten,
		ConsumerErrors:    m.ConsumerErrors,
	})

	mux := http.NewServeMux()
	mux.Handle("POST /ingest", ingest.NewHTTPHandler(buf, logger))
	query.New(pgStore).Register(mux)

	trust := buildTrustFunc(cfg)
	httpStack := httpmiddleware.Chain(
		httpmiddleware.RequestID,
		httpmiddleware.AccessLog(logger.With("component", "http")),
		httpmiddleware.Recover(logger),
		httpmiddleware.MaxBodySize(1<<20),
		gatewayctx.TrustedMiddleware(trust),
	)

	var httpTLS *tlsx.ServerConfig
	if cfg.TLS.CertFile != "" || cfg.TLS.KeyFile != "" {
		tlsCfg := cfg.TLS.ToTLSx()
		httpTLS = &tlsCfg
	}

	httpSrv, err := httpserver.New(&httpserver.Config{
		Addr:         cfg.HTTPAddr(),
		Handler:      httpStack(mux),
		TLS:          httpTLS,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}, logger.With("component", "http_server"))
	if err != nil {
		return fmt.Errorf("http server: %w", err)
	}

	var grpcOpts []grpc.ServerOption
	if cfg.GRPC.TLS.CertFile != "" || cfg.GRPC.TLS.KeyFile != "" {
		tc, err := tlsx.NewServerTLSConfig(cfg.GRPC.TLS.ToTLSx())
		if err != nil {
			return fmt.Errorf("grpc tls: %w", err)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tc)))
		logger.Info("gRPC TLS enabled")
	}

	grpcServer := grpc.NewServer(grpcOpts...)
	auditv1.RegisterAuditIngestServiceServer(grpcServer, ingest.NewGRPC(buf))
	if cfg.GRPC.Reflection {
		reflection.Register(grpcServer)
	}

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddr())
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	rc := health.NewReadinessChecker(Version, true)
	rc.Register("rabbitmq", func() health.CheckStatus {
		if cm.IsConnected() {
			return health.CheckStatus{Status: "ok"}
		}
		return health.CheckStatus{Status: "fail", Detail: "not connected"}
	})
	rc.Register("postgres", func() health.CheckStatus {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(cctx); err != nil {
			return health.CheckStatus{Status: "fail", Detail: err.Error()}
		}
		return health.CheckStatus{Status: "ok"}
	})
	rc.Register("audit_buffer", func() health.CheckStatus {
		capacity := buf.Capacity()
		if capacity == 0 {
			return health.CheckStatus{Status: "ok"}
		}
		depth := buf.Depth()
		if float64(depth)/float64(capacity) >= 0.8 {
			return health.CheckStatus{Status: "degraded", Detail: "buffer over 80% full"}
		}
		return health.CheckStatus{Status: "ok"}
	})

	rc.Register("grpc_server", health.TCPDialCheck(cfg.GRPCAddr(), time.Second))

	healthSidecar, err := health.NewSidecar(health.SidecarConfig{
		Addr:      cfg.HealthAddr(),
		Version:   Version,
		Readiness: rc,
		Metrics:   observe.MetricsHandler(reg),
		Logger:    logger.With("component", "health"),
	})
	if err != nil {
		return fmt.Errorf("health sidecar: %w", err)
	}
	go func() {
		if err := healthSidecar.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("health sidecar error", "error", err)
		}
	}()

	go func() {
		if err := grpcServer.Serve(grpcLis); err != nil {
			logger.Error("grpc server error", "error", err)
		}
	}()

	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server error", "error", err)
		}
	}()

	logger.Info("csar-audit started",
		"http", cfg.HTTPAddr(),
		"grpc", cfg.GRPCAddr(),
		"health", cfg.HealthAddr(),
	)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("shutting down", "signal", fmt.Sprintf("%v", sig))

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	appCancel()

	grpcDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcDone)
	}()
	select {
	case <-grpcDone:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown", "error", err)
	}

	if err := healthSidecar.Shutdown(shutdownCtx); err != nil {
		logger.Error("health sidecar shutdown", "error", err)
	}

	return nil
}

func buildTrustFunc(cfg *config.Config) gatewayctx.TrustFunc {
	cnm := cfg.HTTP.AllowedClientCN
	if cnm == "" {
		return func(*http.Request) error { return nil }
	}
	return func(r *http.Request) error {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			return fmt.Errorf("client certificate required")
		}
		if peer := r.TLS.PeerCertificates[0]; peer.Subject.CommonName != cnm {
			return fmt.Errorf("client CN %q not authorized", peer.Subject.CommonName)
		}
		return nil
	}
}

func declareDLQ(cm *rmq.ConnectionManager, dlq string) error {
	ch, err := cm.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dlq: %w", err)
	}
	return nil
}
