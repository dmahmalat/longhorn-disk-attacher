package main

import (
	"context"
	"fmt"
	"os"
	"time"

	longhornV1Beta1 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/spotahome/kooper/v2/controller"
	kooperLogger "github.com/spotahome/kooper/v2/log"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type Logger struct {
	*zap.SugaredLogger
}

var (
	zapLogger, _ = zap.NewProduction()
	log          = Logger{zapLogger.Sugar()}

	//nodeNamePattern = os.Getenv("NODE_NAME_PATTERN")
	//diskPaths       = os.Getenv("DISK_PATHS")

	refreshDuration = 30
)

// Add functions to satisfy kooper's logging interface
func (l Logger) Warningf(template string, args ...interface{}) {
	l.Warnf(template, args)
}

func (l Logger) WithKV(kv kooperLogger.KV) kooperLogger.Logger {
	return l
}

func run() error {
	// Get Kubernetes/Longhorn config and client
	k8scfg, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error loading kubernetes configuration: %w", err)
	}

	k8scli, err := kubernetes.NewForConfig(k8scfg)
	if err != nil {
		return fmt.Errorf("error creating kubernetes client: %w", err)
	}

	longhorncli, err := longhorn.NewForConfig(k8scfg)
	if err != nil {
		return fmt.Errorf("error creating longhorn client: %w", err)
	}

	// Handler
	hand := controller.HandlerFunc(func(_ context.Context, obj runtime.Object) error {
		deployment, ok := obj.(*appsv1.Deployment)
		if ok {
			log.Infof("Deployment added: %s/%s", deployment.Namespace, deployment.Name)
			return nil
		}

		volume, ok := obj.(*longhornV1Beta1.Volume)
		if ok {
			log.Infof("Volume added: %s/%s", volume.Namespace, volume.Name)
			return nil
		}

		return nil
	})

	// Controller for Longhorn Volumes
	ctrlVolumes, err := controller.New(&controller.Config{
		Name:                 "longhorn-disk-attacher-volume-controller",
		Logger:               log,
		ConcurrentWorkers:    1,
		ProcessingJobRetries: 5,
		ResyncInterval:       time.Duration(refreshDuration) * time.Second,

		Handler: hand,
		Retriever: controller.MustRetrieverFromListerWatcher(&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return longhorncli.LonghornV1beta1().Volumes("").List(context.Background(), options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return longhorncli.LonghornV1beta1().Volumes("").Watch(context.Background(), options)
			},
		}),
	})
	if err != nil {
		return fmt.Errorf("could not create controller: %w", err)
	}

	// Controller for Deployments
	ctrlDeployments, err := controller.New(&controller.Config{
		Name:                 "longhorn-disk-attacher-deployment-controller",
		Logger:               log,
		ConcurrentWorkers:    1,
		ProcessingJobRetries: 5,
		ResyncInterval:       time.Duration(refreshDuration) * time.Second,

		Handler: hand,
		Retriever: controller.MustRetrieverFromListerWatcher(&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return k8scli.AppsV1().Deployments("").List(context.Background(), options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return k8scli.AppsV1().Deployments("").Watch(context.Background(), options)
			},
		}),
	})
	if err != nil {
		return fmt.Errorf("could not create controller: %w", err)
	}

	// Start the controllers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errC := make(chan error)

	go func() {
		errC <- ctrlVolumes.Run(ctx)
	}()

	go func() {
		errC <- ctrlDeployments.Run(ctx)
	}()

	// Wait until one exits
	err = <-errC
	if err != nil {
		return fmt.Errorf("error running controllers: %w", err)
	}

	return nil
}

func main() {
	log.Info("Starting controller.")

	err := run()
	if err != nil {
		log.Fatalf("error running app: %s", err)
	}

	os.Exit(0)
}
