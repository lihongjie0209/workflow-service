package workflowruntime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/lihongjie0209/workflow-service/internal/config"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
)

type Runtime struct {
	Client client.Client
	worker worker.Worker
}

func NewRuntime(lifecycle fx.Lifecycle, cfg config.Config, activities *Activities) (*Runtime, error) {
	if !cfg.Temporal.Enabled {
		return &Runtime{}, nil
	}
	tlsConfig, err := temporalTLS(cfg.Temporal.TLS)
	if err != nil {
		return nil, err
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), cfg.Temporal.ConnectTimeout)
	defer cancel()
	temporalClient, err := client.DialContext(connectCtx, client.Options{
		HostPort: cfg.Temporal.HostPort, Namespace: cfg.Temporal.Namespace, Identity: cfg.App.Name,
		ConnectionOptions: client.ConnectionOptions{TLS: tlsConfig},
	})
	if err != nil {
		return nil, fmt.Errorf("connect Temporal: %w", err)
	}
	temporalWorker := worker.New(temporalClient, cfg.Temporal.TaskQueue, worker.Options{WorkerStopTimeout: cfg.Temporal.WorkerStopTimeout})
	temporalWorker.RegisterWorkflowWithOptions(Execute, temporalworkflow.RegisterOptions{Name: WorkflowName})
	temporalWorker.RegisterActivityWithOptions(activities.CreateApprovalTask, activity.RegisterOptions{Name: ActivityCreateApprovalTask})
	temporalWorker.RegisterActivityWithOptions(activities.InvokeServiceTask, activity.RegisterOptions{Name: ActivityInvokeServiceTask})
	temporalWorker.RegisterActivityWithOptions(activities.CompensateServiceTask, activity.RegisterOptions{Name: ActivityCompensateServiceTask})
	temporalWorker.RegisterActivityWithOptions(activities.EvaluateCondition, activity.RegisterOptions{Name: ActivityEvaluateCondition})
	temporalWorker.RegisterActivityWithOptions(activities.FinishInstance, activity.RegisterOptions{Name: ActivityFinishInstance})
	runtime := &Runtime{Client: temporalClient, worker: temporalWorker}
	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			if err := temporalWorker.Start(); err != nil {
				return fmt.Errorf("start Temporal worker: %w", err)
			}
			return nil
		},
		OnStop: func(context.Context) error {
			temporalWorker.Stop()
			temporalClient.Close()
			return nil
		},
	})
	return runtime, nil
}

func temporalTLS(cfg config.ClientTLS) (*tls.Config, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	result := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.ServerName}
	if cfg.CAFile != "" {
		content, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read Temporal CA: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(content) {
			return nil, errors.New("temporal CA file contains no certificates")
		}
		result.RootCAs = pool
	}
	if cfg.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load Temporal client certificate: %w", err)
		}
		result.Certificates = []tls.Certificate{certificate}
	}
	return result, nil
}
