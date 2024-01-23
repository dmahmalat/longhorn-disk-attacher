package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	longhornV1Beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/spotahome/kooper/v2/controller"
	kooperLogger "github.com/spotahome/kooper/v2/log"
	"go.uber.org/zap"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type Logger struct {
	*zap.SugaredLogger
}

type Disk struct {
	name string
	path string
}

var (
	zapLogger, _ = zap.NewProduction()
	log          = Logger{zapLogger.Sugar()}

	nodeNamePattern  = os.Getenv("NODE_NAME_PATTERN")
	diskConfigString = os.Getenv("DISK_CONFIG")

	refreshDuration = 30
)

// Add functions to satisfy kooper's logging interface
func (l Logger) Warningf(template string, args ...interface{}) {
	l.Warnf(template, args)
}

func (l Logger) WithKV(kv kooperLogger.KV) kooperLogger.Logger {
	return l
}

// Read Filesystem File
func readFile(path string) string {
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Error reading file: %s", err)
		return ""
	}

	return strings.TrimSuffix(string(fileBytes), "\n")
}

// Parse Disk Config
func parseDiskConfig(cfg string) ([]Disk, error) {
	disks := []Disk{}

	diskDefinition := strings.Split(strings.ReplaceAll(cfg, " ", ""), ",")
	for _, def := range diskDefinition {
		diskData := strings.Split(def, ":")

		if len(diskData) != 2 {
			return disks, errors.New("error parsing the disk configuration")
		}

		disks = append(disks, Disk{
			name: string(diskData[0]),
			path: string(diskData[1]),
		})
	}

	return disks, nil
}

func run() error {
	// Variables
	namespace := readFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	diskConfig, err := parseDiskConfig(diskConfigString)
	if err != nil {
		return fmt.Errorf("error loading disk configuration: %w", err)
	}

	// Configure Longhorn client
	k8scfg, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error loading kubernetes configuration: %w", err)
	}

	longhorncli, err := longhorn.NewForConfig(k8scfg)
	if err != nil {
		return fmt.Errorf("error creating longhorn client: %w", err)
	}

	// Handler
	handler := controller.HandlerFunc(func(_ context.Context, obj runtime.Object) error {
		node, ok := obj.(*longhornV1Beta2.Node)
		if ok && strings.Contains(node.Name, nodeNamePattern) {
			dataChanged := false

			nodeData := node.DeepCopy()
			nodeDisks := nodeData.Spec.Disks
			for _, diskData := range diskConfig {
				if _, ok := nodeDisks[diskData.name]; !ok {
					nodeDisks[diskData.name] = longhornV1Beta2.DiskSpec{
						Type:              "filesystem",
						Path:              diskData.path,
						AllowScheduling:   true,
						EvictionRequested: false,
						StorageReserved:   0,
						Tags:              []string{},
					}
					dataChanged = true
					nodeData.Spec.Disks = nodeDisks
					log.Infof("Added disk: %s mounted at %s", diskData.name, diskData.path)
				}
			}

			if dataChanged {
				_, err := longhorncli.LonghornV1beta2().Nodes(namespace).Update(context.TODO(), nodeData, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("error updating node: %w", err)
				}

				log.Infof("Node %s updated", node.Name)
			}
		}

		return nil
	})

	// Controller for Longhorn Nodes
	nodeController, err := controller.New(&controller.Config{
		Name:                 "longhorn-disk-attacher-node-controller",
		Logger:               log,
		ConcurrentWorkers:    1,
		ProcessingJobRetries: 5,
		ResyncInterval:       time.Duration(refreshDuration) * time.Second,

		Handler: handler,
		Retriever: controller.MustRetrieverFromListerWatcher(&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return longhorncli.LonghornV1beta2().Nodes(namespace).List(context.Background(), options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return longhorncli.LonghornV1beta2().Nodes(namespace).Watch(context.Background(), options)
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
		errC <- nodeController.Run(ctx)
	}()

	// Wait until one exits
	err = <-errC
	if err != nil {
		return fmt.Errorf("error running controllers: %w", err)
	}

	return nil
}

func main() {
	log.Info("Starting...")

	err := run()
	if err != nil {
		log.Fatalf("error running app: %s", err)
	}

	os.Exit(0)
}
