package s3

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go"
	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/plugin/output/file"
	"go.uber.org/zap"
)

const (
	fileNameSeparator  = "_"
	attemptIntervalMin = 1 * time.Second
)

var (
	attemptInterval = attemptIntervalMin
	compressors     = map[string]func(*zap.SugaredLogger) compressor{
		zipName: newZipCompressor,
	}

	r = rand.New(rand.NewSource(time.Now().UnixNano()))
)

type objectStoreClient interface {
	BucketExists(bucketName string) (bool, error)
	FPutObject(bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error)
}

type compressor interface {
	compress(archiveName, fileName string)
	getObjectOptions() minio.PutObjectOptions
	getExtension() string
	getName(fileName string) string
}

type Plugin struct {
	controller pipeline.OutputPluginController
	logger     *zap.SugaredLogger
	config     *Config
	client     objectStoreClient
	outPlugin  *file.Plugin

	targetDir     string
	fileExtension string
	fileName      string

	compressCh chan string
	uploadCh   chan string

	compressor compressor

	mu *sync.RWMutex
}

type Config struct {
	////can't use struct like this because of parse problems
	//file.Config

	//> File name for log file.
	TargetFile string `json:"target_file" default:"/var/log/file-d.log"` //*

	//> Interval of creation new file
	RetentionInterval  cfg.Duration `json:"retention_interval" default:"1h" parse:"duration"` //*
	RetentionInterval_ time.Duration

	//> Layout is added to targetFile after sealing up. Determines result file name
	Layout string `json:"time_layout" default:"01-02-2006_15:04:05"` //*

	//> How much workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` //*
	WorkersCount_ int

	//> Maximum quantity of events to pack into one batch.
	BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4" parse:"expression"` //*
	BatchSize_ int

	//> After this timeout batch will be sent even if batch isn't completed.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"1s" parse:"duration"` //*
	BatchFlushTimeout_ time.Duration

	//> File mode for log files
	FileMode  cfg.Base8 `json:"file_mode" default:"0666" parse:"base8"` //*
	FileMode_ int64

	//> Compression type
	CompressionType string `json:"compression_type" default:"zip"` //*

	//	s3 section
	Endpoint        string `json:"endpoint" required:"true"`
	AccessKey       string `json:"accessKey" required:"true"`
	SecretAccessKey string `json:"secretKey" required:"true"`
	Bucket          string `json:"bucket" required:"true"`
	Secure          bool   `json:"insecure" default:"false"`
}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    "s3",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.config = config.(*Config)

	//set up compression
	p.config.CompressionType = strings.ToLower(p.config.CompressionType)
	getNewCompressor, ok := compressors[p.config.CompressionType]
	if !ok {
		p.logger.Panicf("compression type: %s is not supported", p.config.CompressionType)
	}
	p.compressor = getNewCompressor(p.logger)

	dir, f := filepath.Split(p.config.TargetFile)

	p.targetDir = dir
	p.fileExtension = filepath.Ext(f)
	p.fileName = f[0 : len(f)-len(p.fileExtension)]

	p.uploadCh = make(chan string, p.config.WorkersCount_)
	p.compressCh = make(chan string, p.config.WorkersCount_)

	for i := 0; i < p.config.WorkersCount_; i++ {
		go p.uploadWork()
		go p.compressWork()
	}
	//Initialize minio client object.
	minioClient, err := minio.New(p.config.Endpoint, p.config.AccessKey, p.config.SecretAccessKey, p.config.Secure)
	if err != nil {
		p.logger.Panicf("could not create minio client, error: %s", err.Error())
	}
	p.client = minioClient

	////now for testing
	//p.client = NewMockClient()
	p.logger.Info("client is ready")

	exist, err := p.client.BucketExists(p.config.Bucket)
	if err != nil {
		p.logger.Panicf("could not check bucket: %s, error: %s", p.config.Bucket, err.Error())
	}
	if !exist {
		p.logger.Panicf("bucket: %s, does not exist", p.config.Bucket)
	}
	p.logger.Infof("bucket: %s exists", p.config.Bucket)

	anyPlugin, _ := file.Factory()
	p.outPlugin = anyPlugin.(*file.Plugin)

	p.outPlugin.AdditionalFunc = p.addFileJob

	p.outPlugin.Start(p.getConfig(), params)
	p.uploadExistedFiles()
}

// getConfig returns file config that was created by data in p.config
func (p *Plugin) getConfig() *file.Config {
	return &file.Config{
		TargetFile:         p.config.TargetFile,
		RetentionInterval:  p.config.RetentionInterval,
		RetentionInterval_: p.config.RetentionInterval_,
		Layout:             p.config.Layout,

		WorkersCount:  p.config.WorkersCount,
		WorkersCount_: p.config.WorkersCount_,

		BatchSize:  p.config.BatchSize,
		BatchSize_: p.config.BatchSize_,

		BatchFlushTimeout:  p.config.BatchFlushTimeout,
		BatchFlushTimeout_: p.config.BatchFlushTimeout_,


		FileMode:  p.config.FileMode,
		FileMode_: p.config.FileMode_,
	}
}

func (p *Plugin) Stop() {
	p.outPlugin.Stop()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.outPlugin.Out(event)
}

// uploadFiles gets files from dirs, sorts it, compresses it if it's need, and then upload to s3
func (p *Plugin) uploadExistedFiles() {
	//compress all files that we have in the dir
	p.compressFilesInDir()
	// get all compressed files
	pattern := fmt.Sprintf("%s*%s", p.targetDir, p.compressor.getExtension())
	compressedFiles, err := filepath.Glob(pattern)
	if err != nil {
		p.logger.Panicf("could not read dir: %s", p.targetDir)
	}
	//sort compressed files by creation time
	sort.Slice(compressedFiles, p.getSortFunc(compressedFiles))
	//upload archive
	for _, z := range compressedFiles {
		p.uploadCh <- z
	}
}

//compressFilesInDir compresses all files in dir
func (p *Plugin) compressFilesInDir() {
	pattern := fmt.Sprintf("%s/%s%s*%s*%s", p.targetDir, p.fileName, fileNameSeparator, fileNameSeparator, p.fileExtension)
	files, err := filepath.Glob(pattern)

	if err != nil {
		p.logger.Panicf("could not read dir: %s", p.targetDir)
	}
	// add sorting
	//sort files by creation time
	sort.Slice(files, p.getSortFunc(files))
	for _, f := range files {
		p.compressor.compress(p.compressor.getName(f), f)
		//delete old file
		if err := os.Remove(f); err != nil {
			p.logger.Panicf("could not delete file: %s, error: %s", f, err.Error())
		}
	}
}

//getSortFunc return func that sorts files by mod time
func (p *Plugin) getSortFunc(files []string) func(i, j int) bool {
	return func(i, j int) bool {
		InfoI, err := os.Stat(files[i])
		if err != nil {
			p.logger.Panicf("could not get info about file: %s, error: %s", files[i], err.Error())
		}
		InfoJ, err := os.Stat(files[j])
		if err != nil {
			p.logger.Panicf("could not get info about file: %s, error: %s", files[j], err.Error())

		}
		return InfoI.ModTime().Before(InfoJ.ModTime())
	}
}

func (p *Plugin) addFileJob(fileName string) {
	fmt.Println("colled after sealing up")
	fmt.Println("fileName-> ", fileName)
	p.compressCh <- fileName
}

//uploadWork uploads compressed files from channel to s3 and then delete compressed file
// in case error worker will attempt sending with an exponential time interval
func (p *Plugin) uploadWork() {
	for compressed := range p.uploadCh {
		sleepTime := attemptInterval
		for {
			err := p.uploadCompressedFile(compressed)
			if err == nil {
				//delete archive after uploading
				err = os.Remove(compressed)
				if err != nil {
					p.logger.Panicf("could not delete file: %s, err: %s", compressed, err.Error())
				}
				break
			}
			p.logger.Errorf("could not upload object: %s, error: %s", compressed, err.Error())
			sleepTime += sleepTime
			time.Sleep(sleepTime)
		}
	}
}

//compressWork compress file from channel and then delete source file
func (p *Plugin) compressWork() {
	for f := range p.compressCh {
		compressedName := p.compressor.getName(f)
		p.compressor.compress(compressedName, f)
		//delete old file
		if err := os.Remove(f); err != nil {
			p.logger.Panicf("could not delete file: %s, error: %s", f, err.Error())
		}
		p.uploadCh <- compressedName
	}
}

//uploadCompressedFile uploads compressed file to s3
func (p *Plugin) uploadCompressedFile(name string) error {
	_, err := p.client.FPutObject(p.config.Bucket, p.generateObjectName(name), name, p.compressor.getObjectOptions())
	if err != nil {
		return fmt.Errorf("could not upload file: %s into bucket: %s, error: %s", name, p.config.Bucket, err.Error())
	}
	return nil
}

//generateObjectName generates object name by compressed file name
func (p *Plugin) generateObjectName(name string) string {
	n := strconv.FormatInt(r.Int63n(math.MaxInt64), 16)
	n = n[len(n)-8:]
	objectName := path.Base(name)
	objectName = objectName[0 : len(objectName)-len(p.compressor.getExtension())]
	return fmt.Sprintf("%s.%s%s", objectName, n, p.compressor.getExtension())
}
