package s3

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
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
	compressedExtension = ".zip"
	fileNameSeparator = "_"
	attemptIntervalMin = 1 * time.Second
)
var (
	options = minio.PutObjectOptions{
		ContentType: "application/zip",
	}

	attemptInterval = attemptIntervalMin
)

type objectStoreClient interface {
	BucketExists(bucketName string) (bool, error)
	FPutObject(bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error)
}

type Plugin struct {
	controller   pipeline.OutputPluginController
	logger       *zap.SugaredLogger
	config       *Config
	client       objectStoreClient
	outPlugin    *file.Plugin

	targetDir     string
	fileExtension string
	fileName      string

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

	dir, f := filepath.Split(p.config.TargetFile)

	p.targetDir = dir
	p.fileExtension = filepath.Ext(f)
	p.fileName = f[0 : len(f)-len(p.fileExtension)]

	// Initialize minio client object.
	//minioClient, err := minio.New(p.config.Endpoint, p.config.AccessKey, p.config.SecretAccessKey, p.config.Secure)
	//if err != nil {
	//	p.logger.Panicf("could not create minio client, error: %s", err.Error())
	//}
	//p.client = minioClient

	//now for testing
	p.client = NewMockClient()
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

	p.outPlugin.AdditionalFunc = p.manageFile

	p.outPlugin.Start(p.getConfig(), params)
	go p.attemptSending()
}

// getConfig returns file config that was created by data in p.config
func (p *Plugin) getConfig() *file.Config{
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

func (p *Plugin) attemptSending() {
	for {
		fmt.Println("call upload from attemp sending")
		if p.uploadExistedFiles() == 0 {
			attemptInterval = attemptIntervalMin
		}
		//increase time interval exponentially
		attemptInterval+=attemptInterval
		time.Sleep(attemptInterval)
	}
}

// mb upload several files into one archive?

// uploadFiles get files from dirs, sorts it and then upload to s3 as zip
func (p *Plugin) uploadExistedFiles() int {
	//compress all files that we have in the dir
	p.compressFilesInDir()
	failed := 0
	// get all zip files
	pattern := fmt.Sprintf("%s*%s", p.targetDir, compressedExtension)
	files, err := filepath.Glob(pattern)

	if err != nil {
		p.logger.Panicf("could not read dir: %s", p.targetDir)
	}

	//sort zip by data
	sort.Slice(files, p.getSortFunc(files))

	//upload archive
	for _, f := range files {
		fmt.Print(f + " ")
		if err := p.uploadZip(f); err != nil {
			failed++
		}
	}
	return failed
}


//compressFilesInDir compresses all files in dir
func (p *Plugin) compressFilesInDir() {
	pattern := fmt.Sprintf("%s/%s%s*%s*%s", p.targetDir, p.fileName, fileNameSeparator, fileNameSeparator, p.fileExtension)
	files, err := filepath.Glob(pattern)

	if err != nil {
		p.logger.Panicf("could not read dir: %s", p.targetDir)
	}
	// add sorting
	//sort zip by data
	sort.Slice(files, p.getSortFunc(files))
	for _, f := range files {
		archiveName := fmt.Sprintf("%s.zip", f)
		p.createZip(archiveName, f)
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

func (p *Plugin) manageFile(fileName string) {
	fmt.Println("colled after sealing up")
	fmt.Println("fileName-> ", fileName)
	zipName := fmt.Sprintf("%s.zip", fileName)
	fmt.Println("archiveName-> ", zipName)

	//compress
	p.createZip(zipName, fileName)

	//upload archive
	if err := p.uploadZip(zipName); err != nil {
		p.logger.Error(err)
	}
}

func (p *Plugin) uploadZip(name string) error {
	fmt.Println("going to upload archive: ", name)
	//Upload the zip file with FPutObject
	var err error = nil

	objectName := path.Base(name)
	_, err = p.client.FPutObject(p.config.Bucket, objectName, name, options)
	if err != nil {
		return fmt.Errorf("could not upload file: %s into bucket: %s, error: %s", name, p.config.Bucket, err.Error())
	}

	fmt.Println("like file was uploaded ")

	//delete archive after uploading
	err = os.Remove(name)
	if err != nil {
		p.logger.Panicf("could not delete file: %s, err: %s", name, err.Error())
	}
	return nil
}

// add file to zip and remove original file
func (p *Plugin) createZip(archiveName string, fileNames ...string) {
	fmt.Println("zip name: ", archiveName)
	newZipFile, err := os.Create(archiveName)
	if err != nil {
		p.logger.Panicf("could not create zip file: %s, error: %s", archiveName, err.Error())
	}
	defer newZipFile.Close()
	zipWriter := zip.NewWriter(newZipFile)
	defer zipWriter.Close()
	for _, f := range fileNames {
		p.addFileToZip(zipWriter, f)
		//delete old file
		if err := os.Remove(f); err != nil {
			p.logger.Panicf("could not delete file: %s, error: %s", f, err.Error())
		}
	}

}

func (p *Plugin) addFileToZip(zipWriter *zip.Writer, filename string) {
	fileToZip, err := os.Open(filename)
	if err != nil {
		p.logger.Panicf("could not open file: %s, error: %s", filename, err.Error())
	}
	defer fileToZip.Close()
	info, err := fileToZip.Stat()
	if err != nil {
		p.logger.Panicf("could not get file inforamtion for file: %s, error: %s", filename, err.Error())
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		p.logger.Panicf("could not set file info header for file: %s, error: %s", filename, err.Error())
	}
	// Using FileInfoHeader() above only uses the basename of the file. If we want
	// to preserve the folder structure we can overwrite this with the full path.
	header.Name = filename

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		p.logger.Panicf("could not create header for file: %s, error: %s", filename, err.Error())
	}
	_, err = io.Copy(writer, fileToZip)
	if err != nil {
		p.logger.Panicf("could not add file: %s to archive, error: %s", filename, err.Error())
	}
}
